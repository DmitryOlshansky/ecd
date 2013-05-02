module hash.lookup;

import alt.blockheap;
import hash.util;
import std.variant;

// The bucket hash field might be the key as well
private struct bucket
{
    bucket *next;
    hash_t hash; 
    /* key   */
    /* value */
}

private alias bucket*  hbucket;

package enum  NodeOp {op_put, op_get, op_del };

alias long ArrayRet_t;

extern (D) typedef int delegate(void *) dg_t;
extern (D) typedef int delegate(void *, void *) dg2_t;


private
{
    import core.stdc.stdarg;
    import core.stdc.string;
    import core.stdc.stdio;
   // import core.stdc.stdlib;

    enum BlkAttr : uint
    {
        FINALIZE = 0b0000_0001,
        NO_SCAN  = 0b0000_0010,
        NO_MOVE  = 0b0000_0100,
        ALL_BITS = 0b1111_1111
    }

    extern (C) void* gc_malloc( size_t sz, uint ba = 0 );
    extern (C) void* gc_calloc( size_t sz, uint ba = 0 );
    extern (C) void  gc_free( void* p );
	extern (C) void* gc_addrOf( void* p );
	
	/**********************************
	 * Align to next pointer boundary, so that
	 * GC won't be faced with misaligned pointers
	 * in value.
	 */

	size_t aligntsize(size_t tsize)
	{
		// Is pointer alignment on the x64 4 bytes or 8?
		return (tsize + size_t.sizeof - 1) & ~(size_t.sizeof - 1);
	}
	
	struct Array
	{
		size_t length;
		void* ptr;
	}
}


class HashLookup
{
    hbucket[] b;			// current data
    size_t nodes;       // total number of entries
    TypeInfo keyti;     //
    hbucket[4] binit;	// initial value of b[]

	size_t keySize_;		// actual number of ubytes in key, which might be less than valueOffset_?
	hash_t keyMask_;       //  required if using hash for integer keys only

	size_t valueOffset_;   // offset from end of aaA to value (key space)
								//TypeInfo aati_;		 // The AssociativeArray typeinfo, which may or may not currently have.
	size_t valueSize_;     // cached size of value
	size_t capacity_;       // for sizing to known limits

	BlockHeap*  heap_;     // optional heap

	double loadRatio_ = 1.0; // after this performance is getting more clogged.

	
	this()
	{
	}
	
	this(TypeInfo_AssociativeArray taa, bool hashSmall = false, uint preAllocate = 0)
	{
		setup(taa.key, taa.next.tsize(), preAllocate, hashSmall);
	}
	
    private final void binitClear()
    {
        for(int i = 0; i < binit.length; i++)
            binit[i] = null;
    }
    // ensure this AA has no nodes, back to init state, retain loadRatio, and TypeInfo,

	private final void wipe()
	{
		binitClear();
		b = binit;
		nodes = 0;
		capacity_  = cast(size_t)(b.length * loadRatio_);
		version(OPTIONAL_BLOCK_HEAP)
		{
		    if (heap_ !is null)
                heap_.clear();
		}

	}
    /**Return a new AA instance holding all data that originally belonged to the old.
    All old aliased references will point to the old, now empty AA.
    */
    @property static  HashLookup assumeUnique(ref HashLookup other)
    {
        HashLookup copy;
		if (other !is null)
		{
			copy = new HashLookup();
			copy.swipe(other);
			return copy;
		}
		return null;
    }	
	package static HashLookup initCopySetup(HashLookup src)
	{
		HashLookup copy = new HashLookup();

		copy.setup(src.keyti, src.valueSize_, 0, (src.valueOffset_ > 0));
		copy.loadRatio_ = src.loadRatio_;
		
        if (src.heap_ !is null)
            copy.capacity(src.nodes);
   
		return copy;
	}
	
	// this could support initialisation by cloning an empty instance?
	private final void swipe(HashLookup other)
	{
	// binit is a headache
		if (other.b.ptr == other.binit.ptr)
		{
			for(int i = 0; i < binit.length; i++)
			{
				binit[i] = other.binit[i];
			}
		}
		else {
			b = other.b;
		}
		nodes = other.nodes;
		other.nodes = 0;

		keyti  = other.keyti;
		keySize_  = other.keySize_;
		keyMask_  = other.keyMask_;
		valueOffset_  = other.valueOffset_;
		valueSize_  = other.valueSize_;
		capacity_  = other.capacity_;
		loadRatio_ = other.loadRatio_;


		version(OPTIONAL_BLOCK_HEAP)
		{
		    heap_ =  other.heap_; // This is where the blocks live.
		    if (heap_ !is null) // no heap sharing, see clear
                other.heap_ = new BlockHeap(heap_);
		}

		other.wipe();
	}

	private void freeNode(hbucket e)
	{
        version(OPTIONAL_BLOCK_HEAP)
        {
            if(heap_ !is null)
                heap_.collect(e);
            else
                gc_free(e);
        }
        else {
            gc_free(e);
        }
	}
	
	// get resulting pointer to stored key
	package final void* getKeyPtr(void* pkey)
	{
		auto e = getNode(pkey, NodeOp.op_put);
		return keyMask_ ? &e.hash : cast(void*)(e+1);
	}
	// encapsulate all the operations for search or change
	package final hbucket getNode(void* pkey, NodeOp op)
	{
        hbucket e = void;
		hash_t key_hash = void;
		hbucket* pe = void;

		if (keyMask_)
		{
			key_hash = *(cast(hash_t*)pkey) & keyMask_;
			pe = &b[key_hash % $];
			for(;;)
			{
				e = *pe;
				if (e is null)
					break;
				if (key_hash == e.hash) // keys match exactly!
				{
					if (op == NodeOp.op_del)
					{
						*pe = e.next;
						nodes--;
					}
					return e;
				}
				pe = &e.next;
			}
		}
		else {
			key_hash =  keyti.getHash(pkey);
			pe = &b[key_hash % $];
			for(;;)
			{
				e = *pe;
				if (e is null)
					break;
				if ((key_hash == e.hash) && (keyti.compare(pkey, e+1)==0))
				{
					// keys match exactly!
					if (op == NodeOp.op_del)
					{
						*pe = e.next;
						nodes--;
					}
					return e;
				}
				pe = &e.next;
			}
		}
        if (op == NodeOp.op_put)
        {
			size_t nsize = bucket.sizeof + valueOffset_ + valueSize_;
			version(OPTIONAL_BLOCK_HEAP)
			{
				if (heap_ is null)
					e = cast(hbucket) gc_calloc(nsize);
				else
					e = cast(hbucket) heap_.allocate();
			}
			else {
				e = cast(hbucket) gc_calloc(nsize);
			}
			if (!keyMask_)
				memcpy(e+1, pkey, keySize_);

            e.hash = key_hash;
            *pe = e;
            nodes++;
			if (nodes > capacity_)
				grow_rehash();
            return e;
        }
        return null; // not found and not create
	}

	private static hash_t getHashMask(size_t nbytes)
	{
		switch(nbytes) // still only 32 bit version, may be extended for 64 bits OS?
		{
		case 1:
			return 0xFF;
		case 2:
			return 0xFFFF;
		case 3:
			return 0xFF_FFFF;
		case 4:
		default:
			return 0xFFFF_FFFF;
		}
	}
	/// this function needs to really positively identity using the runtime TypeInfo,
	/// that the type is integral ( and not a class, or a interface or structure ).

    private void initHeap(size_t initCap)
    {
        heap_ = new BlockHeap(null);
        size_t node_size = bucket.sizeof + valueOffset_ + valueSize_;
        heap_.setup(node_size);
        heap_.preAllocate(initCap);
    }
	
	void setup(TypeInfo ti, size_t valSizeOf, size_t initCap, bool hashSmall = false)
	{
		keyti = ti;
		keySize_ = keyti.tsize();
		valueSize_ = valSizeOf;

		if (!hashSmall && IsOwnHashType(keyti))
		{   // store the key in the hash value
			valueOffset_= 0;
			keyMask_ = getHashMask(keySize_);
		}
		else {
			valueOffset_ = aligntsize(keySize_);
			keyMask_ = 0;
		}

		if (initCap > 0)
		{
			// preallocate nodes for NodeHeap
			capacity(initCap);
		}
		else {
			b = binit;
			capacity_ = cast(size_t)(b.length * loadRatio_);
		}
	}
 	private  void resizeTable(size_t nlen)
	{
        hbucket[] newtable = new hbucket[nlen];

        if (nodes)
        foreach (e; b)
        {
            while(e !is null)
            {
                hbucket aaNext = e.next;
                e.next = null;

                auto key_hash = e.hash;
                hbucket* pe = &newtable[key_hash % $];
                while (*pe !is null)
                {
                    pe = &(*pe).next;
                }
                *pe = e;
                e = aaNext;
           }
        }
        if (b.ptr == binit.ptr)
        {
            binit = null;
        }
        else
            delete b;
        b = newtable;
        capacity_ = cast(size_t)(b.length * loadRatio_);
	}

    void loadRatio(double ratio)
    {
        if (ratio < 0.5)
        {
            ratio = 0.5;
        }
        else if (ratio > 8.0)
        {
            ratio = 8.0;
        }
        loadRatio_ = ratio;
    }
	void capacity(size_t cap)
    {
        version(OPTIONAL_BLOCK_HEAP)
        {
            if (nodes == 0 && cap > 0)
                initHeap(cap);
        }

        if (cap < nodes)
            cap = nodes;
        size_t nlen = cast(size_t)(cap / loadRatio_);

        nlen = getNextPrime(nlen);

        resizeTable(nlen);
    }

	uint[] statistics(){
        uint result[];

        if (b is null)
		{
			result.length = 2;
			result[0] = 0;
			result[1] = 0;
            return result;
		}

        uint emptyCt = 0;

        result.length = 16;
        result[0] = b.length;

		foreach(e ; b)
		{
			if(e !is null)
			{
				uint listct = 0;
				while (e !is null)
				{
					listct++;
					e = e.next;
				}

				if (listct >= result.length-1)
				{
					result.length = listct + 2;
				}
				result[listct+1] += 1;
			}
			else {
				emptyCt++;
			}

		}
        result[1] = emptyCt;
        return result;
    }

    /// capacity exceeded, grow much bigger, avoid frequent rehash
    private void grow_rehash()
    {
        size_t nlen = cast(size_t)(nodes / 0.25);
        nlen = getNextPrime(nlen);
        if (nlen > b.length)
            resizeTable(nlen);
        capacity_ = cast(size_t)(b.length * 4.0);
        return;
    }
    void rehash()
    {
        size_t nlen = cast(size_t)(nodes / loadRatio_);
        nlen = getNextPrime(nlen);
        if (nlen != b.length)
            resizeTable(nlen);
        capacity_ = cast(size_t)(b.length * loadRatio_);
        return;
    }


    void clear()
    {
        version(OPTIONAL_BLOCK_HEAP)
        {   // This also prevents a shared block heap.
            if (heap_ !is null)
            {
                heap_.clear();
                nodes = 0;
            }
        }
	    if (nodes)
	    {
			foreach(e ; b)
			{
				while(e !is null)
				{
					hbucket nxptr = e.next;
					freeNode(e);

					e = nxptr;
				}
			}
		}
		if (b.ptr != binit.ptr)
		{
            delete b;
		}
        wipe();
   }

    size_t keysAndValues(ArrayRet_t* ka, ArrayRet_t* va, TypeInfo valti)
    {
		Array	keyBlock;
        Array	valueBlock;

        if(nodes > 0)
        {
			size_t ix = 0;
			keyBlock.length = nodes;
			keyBlock.ptr = cast(byte*) gc_calloc(nodes * keySize_,
								!(keyti.flags() & 1) ? BlkAttr.NO_SCAN : 0);


            valueBlock.length = nodes;

            bool no_scan =  (valti is null) ? (valueSize_ < (void*).sizeof) :
                            !(valti.flags() & 1);

			valueBlock.ptr = cast(byte*) gc_malloc(nodes * valueSize_,
                                      no_scan ? BlkAttr.NO_SCAN : 0);
            /// maybe could use valueti.flags() if had valueti

            foreach(e ; b)
            {
				if (keyMask_)
				{
					while (e !is null)
					{
						hash_t* keyptr = cast(hash_t*)(keyBlock.ptr + ix * keySize_);
						// 4 or less bytes to set ?, guessing what happens here
						// some debugging may be required
						*keyptr |= (e.hash & keyMask_);

                        memcpy(valueBlock.ptr + ix * valueSize_,
							cast(byte*)(e+1), valueSize_);


						e = e.next;
						ix++;
					}
				}
				else {
					while (e !is null)
					{
						memcpy(keyBlock.ptr + ix * valueOffset_,
								cast(byte*)(e+1), keySize_);

                        memcpy(valueBlock.ptr + ix * valueSize_,
							cast(byte*)(e+1) + valueOffset_, valueSize_);

						e = e.next;
						ix++;
					}
				}
            }
        }
        *ka = *cast(ArrayRet_t*)(&keyBlock);
        *va = *cast(ArrayRet_t*)(&valueBlock);
		return nodes;
    }
 	ArrayRet_t values(TypeInfo valti)
	{
		Array	valueBlock;

        if(nodes > 0)
        {
			size_t ix = 0;

            bool no_scan =  (valti is null) ? (valueSize_ < (void*).sizeof) :
                            !(valti.flags() & 1);

			valueBlock.length = nodes;
			valueBlock.ptr = cast(byte*) gc_malloc(nodes * valueSize_,
                                      no_scan ? BlkAttr.NO_SCAN : 0);
            foreach(e ; b)
            {
                while (e !is null)
                {
				    memcpy(valueBlock.ptr + ix * valueSize_,
							cast(byte*)(e+1) + valueOffset_, valueSize_);

                    e = e.next;
					ix++;
                }
            }
        }

		return *cast(ArrayRet_t*)(&valueBlock);
	}

	ArrayRet_t keys()
	{
		Array	keyBlock;

        if(nodes > 0)
        {
			size_t ix = 0;
			keyBlock.length = nodes;
			keyBlock.ptr = cast(byte*) gc_calloc(nodes * keySize_,
								!(keyti.flags() & 1) ? BlkAttr.NO_SCAN : 0);
            foreach(e ; b)
            {
				if (keyMask_)
				{
					while (e !is null)
					{
						hash_t* keyptr = cast(hash_t*)(keyBlock.ptr + ix * keySize_);
						// 4 or less bytes to set ?, guessing what happens here
						// some debugging may be required
						*keyptr |= (e.hash & keyMask_);
						e = e.next;
						ix++;
					}
				}
				else {
					while (e !is null)
					{
						memcpy(keyBlock.ptr + ix * valueOffset_,
								cast(byte*)(e+1), keySize_);

						e = e.next;
						ix++;
					}
				}
            }
        }
		return *cast(ArrayRet_t*)(&keyBlock);

	}
	int applyValues(dg_t dg)
	{
        int result;
        foreach (e; b)
        {
            while (e !is null)
            {
                hbucket nx = e.next;
				result = dg(cast(void *)(e + 1) + valueOffset_);
                if (result || nodes == 0)
                    break;
                e = nx;
            }
        }
        return result;
	}

	equals_t dataMatch(HashLookup other, TypeInfo_AssociativeArray aati_)
	{
		if (other.nodes != nodes)
			return false;
		auto valueti = aati_.next;
        foreach (e; b)
        {
            if (keyMask_)
			{
				while (e !is null)
				{

					void* pkey = &e.hash;
					hbucket test = other.getNode(pkey, NodeOp.op_get);
					if (test is null)
						return false;
					void* pvalue = cast(void*)(e + 1);
					void* pvalue2 = cast(void*)(test+1);
					if (!valueti.equals(pvalue, pvalue2))
						return false;
					e = e.next;
				}
			}
			else {
				while (e !is null)
				{

					void* pkey = cast(void *)(e + 1);
					hbucket test = other.getNode(pkey, NodeOp.op_get);
					if (test is null)
						return false;

					void* pvalue = pkey + valueOffset_;
					void* pvalue2 = cast(void*)(test+1) + valueOffset_;
					if (!valueti.equals(pvalue, pvalue2))
						return false;
					e = e.next;
				}
			}
        }
        return true;
	}

	int applyKeyValues(dg2_t dg)
	{
        int result;
		if (keyMask_)
		{
			foreach (e; b)
			{
				while (e !is null)
				{
					hbucket nx = e.next;
					void* pkey = cast(void *)(&e.hash);
					result = dg(pkey, cast(void *)(e+1));
					if (result || nodes == 0)
						break;
					e = nx;
				}
			}
		}
		else {
			foreach (e; b)
			{
				while (e !is null)
				{
					hbucket nx = e.next;
					void* pkey = cast(void *)(e + 1);
					result = dg(pkey, pkey + valueOffset_);
					if (result || nodes == 0)
						break;
					e = nx;
				}
			}		}
        return result;
	}

	int applyKeys(dg_t dg)
	{
        int result;
		if (keyMask_)
		{
			foreach (e; b)
			{
				while (e !is null)
				{
					hbucket nx = e.next;
					hash_t temp = e.hash; // the temporary can be safely altered
					result = dg(&temp);
					if (result || nodes == 0)
						break;
					e = nx;
				}
			}
		}
		else {
		    // make a temporary key buffer
		    immutable ksize = keySize_;
			void* keybuf = core.stdc.stdlib.alloca(ksize);

			foreach (e; b)
			{
				while (e !is null)
				{
					hbucket nx = e.next;
					memcpy(keybuf, cast(void *)(e + 1), ksize);
					result = dg(keybuf); /// not getting the original

					if (result || nodes == 0)
						break;
					e = nx;
				}
			}
			gc_free(keybuf);
        }
        return result;
	}

}

private HashLookup initHashLookup(TypeInfo_AssociativeArray ti, bool hashSmall)
{

    auto valuesize = ti.next.tsize();           // value size
    auto keyti = ti.key;
    HashLookup impl = new HashLookup();
    impl.setup(keyti, valuesize, 0, hashSmall);
    return impl;
}


struct HashTable(V : V[K], K) {
    private HashLookup aa_;

    /** A call to setup always creates a new implementation object, overwriting the
        instance created by a previous setup.
        Overwriting does not delete pre-existing implementation object, which
        may be aliased.

        ---
        DRHashMap!(uint[uint]) aa;

        aa.setup; // optional setup, default, do not hash integer/float/char keys
        ---

        OR
         ---
        DRHashMap!(uint[uint]) aa;

        aa.setup(true); //optional setup, use normal hashing.
        ---

       If hashSmall is false (default), then key integer bitty values are stored
       directly in the hash field, as themselves without doing a hash.
       This can be a bit faster for these keys.

       The call to setup will be automatic if it is not called first deliberatedly.

    */


    void setup(bool hashSmall = false, uint preAllocate = 0)
    {
		aa_ = new HashLookup(
			cast(TypeInfo_AssociativeArray)typeid(V[K]), hashSmall, preAllocate);
    }

    /// Return if the implementation is already initialised.
    bool isSetup()
    {
        return ( aa_ !is null);
    }

    /// support "in" operator
    V* opIn_r(K key)
    {

	   if (aa_ is null)
		   return null;
	   hbucket e = aa_.getNode(cast(void*) &key, NodeOp.op_get);
	   return cast(V*) (e is null ? null : cast(void *)(e + 1) + aa_.valueOffset_);
    }

    /// Return the value or throw exception
    V opIndex(K key)
    {
		if (aa_ !is null)
		{
			hbucket e = aa_.getNode(cast(void*) &key, NodeOp.op_get);
            if (e !is null)
            {
                return *(cast(V*)(cast(void *)(e + 1) + aa_.valueOffset_));
            }
        }
        throw new AAKeyError("no key for opIndex");
    }

    /// Insert or replace. Will call setup for uninitialised AA.
    void opIndexAssign(V value, K key)
    {
        if (aa_ is null)
            setup();
		hbucket e = aa_.getNode(&key, NodeOp.op_put);
        auto pval = cast(V*)(cast(void *)(e + 1) + aa_.valueOffset_);
        *pval = value;
    }


    /// Insert or replace. Return true if insert occurred.
    bool putInsert(K key, ref V value)
    {
        if (aa_ is null)
            setup();
        size_t before_nodes = aa_.nodes;
		hbucket e = aa_.getNode(&key, NodeOp.op_put);
        auto pval = cast(V*)(cast(void *)(e + 1) + aa_.valueOffset_);
        *pval = value;
        return (aa_.nodes > before_nodes);
    }

    /// Insert or replace.
    void put(K key, ref V value)
    {
        if (aa_ is null)
            setup();

		hbucket e = aa_.getNode(&key, NodeOp.op_put);
        auto pval = cast(V*)(cast(void *)(e + 1) + aa_.valueOffset_);
		*pval = value;
    }

    /// Get the value or throw exception
    V get(K key)
    {
		if (aa_ !is null)
		{
			hbucket e = aa_.getNode(cast(void*) &key, NodeOp.op_get);
            if (e !is null)
            {
                return *(cast(V*)(cast(void *)(e + 1) + aa_.valueOffset_));
            }
        }
        throw new AAKeyError("AA get failed");
    }
    /// Return if the key exists.
    bool contains(K key)
    {
        if (aa_ is null)
            return false;
        return  aa_.getNode(cast(void*) &key, NodeOp.op_get) !is null;
    }
    /// Get the value if it exists, false if it does not.
    bool get(K key, ref V val)
    {
        if (aa_ !is null)
        {
			hbucket e = aa_.getNode(&key, NodeOp.op_get);
            if (e !is null)
            {
                val = *(cast(V*)(cast(void *)(e + 1) + aa_.valueOffset_));
                return true;
            }
        }
        static if(!is(V == Variant))
            val = V.init;
        return false;
    }

    /**
        Set the capacity, which cannot be made less than current number of entries.
        The actual capacity value achieved will usually larger.
        Table length is set to be (next prime number) > (capacity / load_ratio).
        Capacity is then set to be (Table length) * load_ratio;
    */
    @property void capacity(size_t cap)
    {
        //version(TEST_DRAA) writefln("capacity %s ",cap);
       if (aa_ is null)
            setup();

        aa_.capacity(cap);
    }
    /** Return threshold number of entries for automatic rehash after insertion.
    */
    @property size_t capacity()
    {
       if (aa_ is null)
         return 0;
       return aa_.capacity_;
    }

    /**
        Set a floating point value to indicate maximum load ratio to
        use when setting capacity, or calling rehash directly.

        Value is coerced between 0.5 and 8.0.
        If insertion causes number of entries to be greater than capacity,
        a value of 0.25 is used to resize, and 4.0 to set maximum capacity.
    */
    @property void loadRatio(double ratio)
    {
        if (aa_ is null)
            setup();
		aa_.loadRatio(ratio);
    }

    /**
        Return the current loadRatio
    */
    @property double loadRatio()
    {
        if (aa_ is null)
            return 0.0;
        else
            return aa_.loadRatio_;
    }
    /**
        Return the number of entries
    */
    @property final size_t length()
    {
		return (aa_ is null) ? 0 : aa_.nodes;
    }

    /** Return a new managed duplicate of all the entries */

    @property HashTable dup()
    {
        HashTable copy;
        if (aa_ is null)
            return copy;

        copy.aa_ = HashLookup.initCopySetup(aa_);

        copy.append(this);

        return copy;
    }

    /**
        Call remove for each entry. Start afresh.
    */

    @property void clear()
    {
        if (aa_ !is null)
        {
            aa_.clear();
        }
    }

    /**
        Optimise table size according to current number of nodes and loadRatio.
    */
    @property void rehash()
    {
       if (aa_ !is null)
       {
           aa_.rehash();
       }
    }
    /// Test if both arrays are empty, the same object, or both have same data
    bool equals(ref HashTable other)
    {
		HashLookup c1 = aa_;
		HashLookup c2 = other.aa_;

		if (c1 is null)
			return (c2 is null);
		else
			if (c2 is null)
				return false;

		return c1.dataMatch(c2,typeid(V[K]));
    }

    /// Non-optimised function to append all the keys from other onto this.
    void append(ref HashTable other)
    {
        if (!other.isSetup())
            return;
        if (aa_ is null)
            setup();

        foreach(key, value ; other)
        {
            this[key] = value;
        }

    }
    /// Delete the key and return true if it existed.
    bool remove(K key)
    {
        if (aa_ is null)
			return false;
		hbucket e = aa_.getNode(cast(void *)&key,NodeOp.op_del);
		if (e !is null)
		{
            aa_.freeNode(e);
		    return true;
		}
		return false;
    }

    /** Return the value and remove the key at the same time.
        Return false if no key found.
    */

    bool remove(K key, ref V value)
    {
        if (aa_ !is null)
        {
			hbucket e = aa_.getNode(cast(void *)(&key),NodeOp.op_del);
			if (e !is null)
			{
				value = *(cast(V*)(cast(void *)(e + 1) + aa_.valueOffset_));
				aa_.freeNode(e);
				return true;
			}
		}
        static if(!is(V == Variant))
            value = V.init;
        return false;
    }

    public int eachKey(int delegate(ref K key) dg)
    {
		return (aa_ is null) ? 0 : aa_.applyKeys(cast(dg_t) dg);
    }
    /**
        foreach(value)
    */
    public int opApply(int delegate(ref V value) dg)
    {
		return (aa_ is null) ? 0 :  aa_.applyValues(cast(dg_t) dg);
    }
    /**
        foreach(key, value)
    */
    public int opApply(int delegate(ref K key, ref V value) dg)
    {
		return (aa_ is null) ? 0 : aa_.applyKeyValues(cast(dg2_t) dg);
    }

    /**
        Return all keys.
    */
    @property
    K[] keys()
    {
        if (aa_ is null)
            return null;
		ArrayRet_t va =  aa_.keys();
		return *(cast(K[]*) &va);
    }
    /**
        Return all values.
    */
    @property
    V[] values()
    {
        if (aa_ is null)
            return null;
		ArrayRet_t va =  aa_.values( typeid(V));
		return *(cast(V[]*) &va);
    }

    /**
        Return all keys and values.
    */
    size_t keysValues(ref K[] ka, ref V[] va)
    {
        if (aa_ is null)
        {
            ka = null;
            va = null;
            return 0;
        }
		return aa_.keysAndValues(cast(ArrayRet_t*) &ka, cast(ArrayRet_t*) &va, typeid(V));
    }

    /**
        Return <hash table length>, <empty buckets>,  <buckets of length 1>, [<buckets of length #>]
        Result will be of length 2 or more.
    */
    @property uint[] list_stats()
    {
		if (aa_ is null)
		{
			return new uint[2];
		}
		else {
			return aa_.statistics();
		}
    }
}


struct HashSet(K)
{
    private HashLookup aa_;

    void setup(bool hashSmall = false, uint preAllocate = 0)
    {
		aa_ = new HashLookup();
		aa_.setup(typeid(K), 0, preAllocate, hashSmall);
    }
	
    bool isSetup()
    {
        return ( aa_ !is null);
    }

    bool contains(K key)
    {
		return (aa_ is null ) ? false : aa_.getNode(cast(void*) &key, NodeOp.op_get) !is null;
    }


    /// Insert key if not already present.  Return if it already existed.
    bool put(K key)
    {
        if (aa_ is null)
            setup();
		auto preNodes = aa_.nodes;
		hbucket e = aa_.getNode(&key, NodeOp.op_put);
        return (preNodes < aa_.nodes);
    }
	/** This is a special get that puts in the value if it does not exist already.
		For only using one copy of a key, always return the original.
	    
	**/
	K get(K key)
	{
	    if (aa_ is null)
            setup();	
		return  *(cast(K*) aa_.getKeyPtr(&key));
	}
    /// Remove key if it exists. Return if already existed.
    bool remove(K key)
    {
        if (aa_ is null)
			return false;
		hbucket e = aa_.getNode(cast(void *)&key,NodeOp.op_del);
		if (e !is null)
		{
            aa_.freeNode(e);
		    return true;
		}
		return false;
    }


    /**
        Return all keys.
    */
    @property
    K[] keys()
    {
        if (aa_ is null)
            return null;
        ArrayRet_t ka = aa_.keys();
        return *(cast(K[]*) &ka);
    }

    public int opApply(int delegate(ref K key) dg)
    {
		return (aa_ is null) ? 0 : aa_.applyKeys(cast(dg_t) dg);
    }

    /// Return if the value exists or not.
    bool opIndex(K key)
    {
		return (aa_ is null ) ? false : aa_.getNode(cast(void*) &key, NodeOp.op_get) !is null;
    }

    /// assign true or false.  false == remove
    void opIndexAssign(bool value, K key)
    {
        if (value)
        {
            if (aa_ is null)
                setup();
            aa_.getNode(&key, NodeOp.op_put);
        }
        else {
            if (aa_ !is null)
               aa_.getNode(&key, NodeOp.op_del);
        }
    }

   /**Return a new hashset instance holding all data that originally belonged to the old.
    All old aliased references will point to the old, now empty AA.
    */
    @property static HashLookup assumeUnique(ref HashLookup other)
    {
        HashLookup copy;
		if (other !is null)
		{
			copy = new HashLookup();
			copy.swipe(other);
			return copy;
		}
		return null;
    }
    /**
        Set the capacity, which cannot be made less than current number of entries.
        The actual capacity value achieved will usually larger.
        Table length is set to be (next prime number) > (capacity / load_ratio).
        Capacity is then set to be (Table length) * load_ratio;
    */
    @property void capacity(size_t cap)
    {
        if (aa_ is null)
            setup();
        aa_.capacity(cap);
    }
    /** Return threshold number of entries for automatic rehash after insertion.
    */
    @property size_t capacity()
    {
       return (aa_ is null) ? 0 : aa_.capacity_;
    }

    /**
        Set a floating point value to indicate maximum load ratio to
        use when setting capacity, or calling rehash directly.

        Value is coerced between 0.5 and 8.0.
        If insertion causes number of entries to be greater than capacity,
        a value of 0.25 is used to resize, and 4.0 to set maximum capacity.
    */
    @property void loadRatio(double ratio)
    {
        if (aa_ is null)
            setup();
        aa_.loadRatio(ratio);
    }

    /**
        Return the current loadRatio
    */
    @property double loadRatio()
    {
		return (aa_ is null) ? 0.0 : aa_.loadRatio_;
    }
    /**
        Return the number of entries
    */
    @property final size_t length()
    {
        return (aa_ is null) ? 0 : aa_.nodes;
    }
    /**
        Return <hash table length>, <empty buckets>,  <buckets of length 1>, [<buckets of length #>]
        Result will be of length 2 or more.
    */
    @property uint[] list_stats()
    {
		return (aa_ is null) ? new uint[2] : aa_.statistics();
    }

    /** Return a new managed duplicate of all the entries */

    @property HashSet dup()
    {
        HashSet copy;
        if (aa_ is null)
            return copy;

        copy.aa_ = HashLookup.initCopySetup(aa_);

        copy.append(this);

        return copy;
    }

    /**
        Call remove for each entry. Start afresh.
    */

    @property void clear()
    {
        if (aa_ !is null)
        {
            aa_.clear();
        }
    }

    /**
        Optimise table size according to current number of nodes and loadRatio.
    */
    @property void rehash()
    {
       if (aa_ !is null)
       {
           aa_.rehash();
       }
    }
    /// Non-optimised function to append all the keys from other onto this.
    void append(ref HashSet other)
    {
        if (!other.isSetup())
            return;
        if (aa_ is null)
            setup();

        foreach(key ; other)
        {
            aa_.getNode(&key, NodeOp.op_put);
        }
    }
}


