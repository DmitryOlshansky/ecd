/**
 * Implementation of associative arrays.
 *
 * Copyright: Copyright Digital Mars 2000 - 2010.
 * License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Walter Bright, Sean Kelly
 *
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
module rt.aaA;

//version=TEST_BED;

private
{
    import core.stdc.stdarg;
    import core.stdc.string;
    import core.stdc.stdio;
    import core.stdc.stdlib;

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
}

// prime values were taken from gnu\hash.c
private immutable
size_t[] prime_list = [
 	53UL, 97UL, 193UL, 389UL,
 	769UL, 1543UL, 3079UL, 6151UL,
 	12289UL, 24593UL, 49157UL, 98317UL,
 	196613UL, 393241UL, 786433UL, 1572869UL,
 	3145739UL, 6291469UL, 12582917UL, 25165843UL,
 	50_331_653UL, 100_663_319UL, 201_326_611UL, 402_653_189UL,
 	805_306_457UL, 1_610_612_741UL,3_221_225_473UL, 4_294_967_291UL
];


private size_t getNextPrime(size_t atLeast)
{
   foreach(p ; prime_list)
   {
       if (p >= atLeast)
        return p;
   }
   throw new Exception("getNextPrime failed");
}

/* This is the type of the return value for dynamic arrays.
 * It should be a type that is returned in registers.
 * Although DMD will return types of Array in registers,
 * gcc will not, so we instead use a 'long'.
 */
alias long ArrayRet_t;

struct Array
{
    size_t length;
    void* ptr;
}

struct aaA
{
    aaA *next;
    hash_t hash;
    /* key   */
    /* value */
}
private alias aaA* aaPtr;


enum MangleTI : char
{
    Tvoid     = 'v',
    Tbool     = 'b',
    Tbyte     = 'g',
    Tubyte    = 'h',
    Tshort    = 's',
    Tushort   = 't',
    Tint      = 'i',
    Tuint     = 'k',
    Tlong     = 'l',
    Tulong    = 'm',
    Tfloat    = 'f',
    Tdouble   = 'd',
    Treal     = 'e',

    Tifloat   = 'o',
    Tidouble  = 'p',
    Tireal    = 'j',
    Tcfloat   = 'q',
    Tcdouble  = 'r',
    Tcreal    = 'c',

    Tchar     = 'a',
    Twchar    = 'u',
    Tdchar    = 'w',

    Tarray    = 'A',
    Tsarray   = 'G',
    Taarray   = 'H',
    Tpointer  = 'P',
    Tfunction = 'F',
    Tident    = 'I',
    Tclass    = 'C',
    Tstruct   = 'S',
    Tenum     = 'E',
    Ttypedef  = 'T',
    Tdelegate = 'D',

    Tconst    = 'x',
    Tinvariant = 'y',
}

// value types that might fit into the 32-bit space of hash_t,
// and can do without special compare or hash functions
private immutable MangleTI hashTypeMangles[] =
[
    MangleTI.Tbool,
	MangleTI.Tbyte, MangleTI.Tubyte, MangleTI.Tshort, MangleTI.Tushort,
	MangleTI.Tint, MangleTI.Tuint, MangleTI.Tifloat,
    MangleTI.Tchar, MangleTI.Twchar, MangleTI.Tdchar,
	MangleTI.Tpointer
];

extern (D) typedef int delegate(void *) dg_t;
extern (D) typedef int delegate(void *, void *) dg2_t;

bool _aaDirectHashType(TypeInfo ifti)
{
    if (ifti.tsize() > hash_t.sizeof)
        return false;

    auto m = cast(MangleTI)ifti.classinfo.name[9];
    foreach(im ; hashTypeMangles)
    {
        if (m == im)
            return true;
    }
    return false;
}


private enum  NodeOp {op_put, op_get, op_del };

version = OPTIONAL_BLOCK_HEAP;
/**
	Adapted from Tango Container module GCChunk. Optional Node Heap storage
*/
version(OPTIONAL_BLOCK_HEAP )
{
	struct BlockHeap // heap of chunks of same sized blocks
	{


		struct element
		{
			element *next;
		}

		struct chunkHeader
		{
			/**
			 * The next chunk in the chain
			 */
			chunkHeader *next;
			/**
			 * The previous chunk in the chain.  Required for O(1) removal
			 * from the chain.
			 */
			chunkHeader *prev;

			/**
			 * The linked list of free elements in the chunk.  This list is
			 * amended each time an element in this chunk is freed.
			 */
			element *freeList;

			/**
			 * The number of free elements in the freeList.  Used to determine
			 * whether this chunk can be given back to the GC
			 */
			uint numFree;

			/**
			 * Allocate a T* from the free list.
			 */
			void *allocateFromFree()
			{
				element *x = freeList;
				freeList = x.next;
				//
				// clear the pointer, this clears the element as if it was
				// newly allocated
				//
				x.next = null;
				numFree--;
				return cast(void*)x;
			}

			// return number of free nodes
			size_t deallocate(void *t, size_t nsize)
			{
				//
				// clear the element so the GC does not interpret the element
				// as pointing to anything else.
				//
				memset(t, 0, nsize);
				element *x = cast(element *)t;
				x.next = freeList;
				freeList = x;
				return (++numFree);
			}

		}
		size_t chunkSize_;
		size_t nodeSize_;  // must be greater or equal to void*


	   /**
		 * The chain of used chunks.  Used chunks have had all their elements
		 * allocated at least once.
		 */
		chunkHeader *used;

		/**
		 * The fresh chunk.  This is only used if no elements are available in
		 * the used chain.
		 */
		chunkHeader *fresh;

		/**
		 * The next element in the fresh chunk.  Because we don't worry about
		 * the free list in the fresh chunk, we need to keep track of the next
		 * fresh element to use.
		 */
		uint nextFresh;

		this(BlockHeap* hp)
		{
		    if (hp !is null)
		    {
                nodeSize_ = hp.nodeSize_;
                chunkSize_ = hp.chunkSize_;
		    }
		}

		void setup(size_t nsize, size_t chunk = 0)
		{
			nodeSize_ = nsize;
			// fit in a 4 K block ? chunkHeader
			// practical upper limit to chunkSize_?
			chunkSize_ = (chunk == 0) ? (4095 - ((void *).sizeof * 3) - uint.sizeof) / nsize : chunk;
		}

		void clear()
		{
			// pre-emptive strike out the entire chain
			chunkHeader*  val;
			val = used;
			while (val !is null)
			{
				chunkHeader* nextUsed = val.next;
				delete val;
				val = nextUsed;
				if (used == val)
					break;
			}
			if (fresh !is null)
				delete fresh;

			used = null;
			fresh = null;
		}
		/**
		 * Allocate a T*
		 */
		void* allocate()
		{
			if(used !is null && used.numFree > 0)
			{
				//
				// allocate one element of the used list
				//
				void* result = used.allocateFromFree();
				if(used.numFree == 0)
					//
					// move used to the end of the list
					//
					used = used.next;
				return result;
			}

			//
			// no used elements are available, allocate out of the fresh
			// elements
			//
			if(fresh is null)
			{
				fresh = cast(chunkHeader*) gc_calloc( chunkHeader.sizeof + nodeSize_ * chunkSize_);
				nextFresh = 0;
			}

			void*  result = cast(void*) (fresh + 1) + nodeSize_ * nextFresh;
			if(++nextFresh == chunkSize_)
			{
				if(used is null)
				{
					used = fresh;
					fresh.next = fresh;
					fresh.prev = fresh;
				}
				else
				{
					//
					// insert fresh into the used chain
					//
					fresh.prev = used.prev;
					fresh.next = used;
					fresh.prev.next = fresh;
					fresh.next.prev = fresh;
					if(fresh.numFree != 0)
					{
						//
						// can recycle elements from fresh
						//
						used = fresh;
					}
				}
				fresh = null;
			}
			return result;
		}
		/+
		void*[] allocate(uint count)
		{
			return new void*[count];
		}
		+/
		// add at least nNodes to the used list
		void preAllocate(size_t nNodes)
		{
			// allocate chunks and setup used linked lists, add to used
			uint alloc_chunks = (nNodes + chunkSize_-1)/ chunkSize_;

			for(uint i = 0; i < alloc_chunks; i++)
			{
				auto hdr = cast(chunkHeader*) gc_calloc( chunkHeader.sizeof + nodeSize_ * chunkSize_);
				void* p = cast(void*)(hdr+1);
				for(uint k = 0; k < chunkSize_; k++, p += nodeSize_)
				{
					element *x = cast(element *)p;
					x.next = hdr.freeList;
					hdr.freeList = x;
				}
				hdr.numFree = chunkSize_;
				if(used is null)
				{
					used = hdr;
					hdr.next = hdr;
					hdr.prev = hdr;
				}
				else
				{
					hdr.prev = used.prev;
					hdr.next = used;
					hdr.prev.next = hdr;
					hdr.next.prev = hdr;
					used = hdr;
				}
			}
		}
		/**
		 * free a T*
		 */
		void collect(void* t)
		{
			//
			// need to figure out which chunk t is in
			//
			chunkHeader *cur = cast(chunkHeader *)gc_addrOf(t);

			if(cur !is fresh && cur.numFree == 0)
			{
				//
				// move cur to the front of the used list, it has free nodes
				// to be used.
				//
				if(cur !is used)
				{
					if(used.numFree != 0)
					{
						//
						// first, unlink cur from its current location
						//
						cur.prev.next = cur.next;
						cur.next.prev = cur.prev;

						//
						// now, insert cur before used.
						//
						cur.prev = used.prev;
						cur.next = used;
						used.prev = cur;
						cur.prev.next = cur;
					}
					used = cur;
				}
			}

			if(cur.deallocate(t, nodeSize_) == chunkSize_)
			{
				//
				// cur no longer has any elements in use, it can be deleted.
				//
				if(cur.next is cur)
				{
					//
					// only one element, don't free it.
					//
				}
				else
				{
					//
					// remove cur from list
					//
					if(used is cur)
					{
						//
						// update used pointer
						//
						used = used.next;
					}
					cur.next.prev = cur.prev;
					cur.prev.next = cur.next;
					delete cur;
				}
			}
		}

		void collect(void*[] t)
		{
			if(t !is null)
				delete t;
		}

		/**
		 * Deallocate all chunks used by this allocator.  Depends on the GC to do
		 * the actual collection
		 */
		bool collect(bool all = true)
		{
			used = null;

			//
			// keep fresh around
			//
			if(fresh !is null)
			{
				nextFresh = 0;
				fresh.freeList = null;
			}

			return true;
		}
	}
}

// This structure manages heaps of memory, so every effort to do it better, even caching various extra pieces of information,
// might have some benefit. Its even nice to write this things in a more D object oriented style. After all this is the DPL.
struct BB
{
	// Make it easier for the code readers, after all, this is the D programming language.


    aaPtr[] b;
    size_t nodes;       // total number of aaA nodes
    TypeInfo keyti;     //
    //TypeInfo aati_;   // TODO: TypeInfo_AssociativeArray
    aaPtr[4] binit;	// initial value of b[]

	size_t keySize_;		// actual number of ubytes in key, which might be less than valueOffset_?
	hash_t keyMask_;       //  required if using hash for integer keys only

	size_t valueOffset_;   // offset from end of aaA to value (key space)
								//TypeInfo aati_;		 // The AssociativeArray typeinfo, which may or may not currently have.
	size_t valueSize_;     // cached size of value
	size_t capacity_;       // for sizing to known limits

	version(OPTIONAL_BLOCK_HEAP)
		BlockHeap*  heap_;     // optional heap

    version(TEST_BED) bool   debug_;

	double loadRatio_ = 1.0; // after this performance is getting more clogged.


    private void binitClear()
    {
        for(int i = 0; i < binit.length; i++)
            binit[i] = null;
    }
    // ensure this AA has no nodes, back to init state, retain loadRatio, and TypeInfo,

	private void wipe()
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

	// this could support initialisation by cloning an empty instance?
	void swipe(BB* other)
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

	private void freeNode(aaPtr e)
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
	// encapsulate all the operations for searching here in the one function
	aaPtr getNode(void* pkey, NodeOp op)
	{
        aaPtr e = void;
		hash_t key_hash = void;
		aaPtr* pe = void;

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
			size_t nsize = aaA.sizeof + valueOffset_ + valueSize_;
			version(OPTIONAL_BLOCK_HEAP)
			{
				if (heap_ is null)
					e = cast(aaPtr) gc_calloc(nsize);
				else
					e = cast(aaPtr) heap_.allocate();
			}
			else {
				e = cast(aaPtr) gc_calloc(nsize);
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

	static hash_t getHashMask(size_t nbytes)
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

    version(OPTIONAL_BLOCK_HEAP)
    {
        private void initHeap(size_t initCap)
        {
            heap_ = new BlockHeap(null);
            size_t node_size = aaA.sizeof + valueOffset_ + valueSize_;
            heap_.setup(node_size);
            heap_.preAllocate(initCap);
        }
    }

	void initBB(TypeInfo ti, size_t valSizeOf, size_t initCap, bool hashSmall = false)
	{
		keyti = ti;
		keySize_ = keyti.tsize();
		valueSize_ = valSizeOf;

		if (!hashSmall && _aaDirectHashType(keyti))
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
        aaPtr[] newtable = new aaPtr[nlen];

        if (nodes)
        foreach (e; b)
        {
            while(e !is null)
            {
                aaPtr aaNext = e.next;
                e.next = null;

                auto key_hash = e.hash;
                aaPtr* pe = &newtable[key_hash % $];
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
					aaA* nxptr = e.next;
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
                aaPtr nx = e.next;
				result = dg(cast(void *)(e + 1) + valueOffset_);
                if (result || nodes == 0)
                    break;
                e = nx;
            }
        }
        return result;
	}

	equals_t dataMatch(BB* other, TypeInfo_AssociativeArray aati_)
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
					aaPtr test = other.getNode(pkey, NodeOp.op_get);
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
					aaPtr test = other.getNode(pkey, NodeOp.op_get);
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
					aaPtr nx = e.next;
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
					aaPtr nx = e.next;
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
					aaPtr nx = e.next;
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
					aaPtr nx = e.next;
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

/* This is the type actually seen by the programmer, although
 * it is completely opaque.
 */

struct AA
{
    BB* a;
}


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

extern (C):



/****************************************************
 * Determine number of entries in associative array.
 */

size_t _aaLen(AA aa)
{
    return aa.a ? aa.a.nodes : 0;
}

/// warning this assumes initialization. Calling routines must check first!
void* _aaPutInsert(AA aa, ...)
{
    BB* impl = aa.a;
	aaPtr e = impl.getNode(cast(void *)(&aa + 1), NodeOp.op_put);
	return (e is null ? null : cast(void *)(e + 1) + impl.valueOffset_);
}

/*************************************************
 * Get pointer to value in associative array indexed by key.
 * Add entry for key if it is not already there.
 * As this is a default kind of initialization, it will get the default
 * setup for storing a real hash for types of integer size or less
 */
void* _aaGet(AA* aa, TypeInfo keyti, size_t valuesize, ...)
{
    auto pkey = cast(void *)(&valuesize + 1);

	BB* impl = aa.a;
    if (impl is null)
    {
		impl = _aaInitKey(keyti, valuesize, true, 0); // using old interface, hash all, no heap
		aa.a = impl;
    }
	aaPtr e = impl.getNode(cast(void *)(&valuesize + 1), NodeOp.op_put);
	return (e is null ? null : cast(void *)(e + 1) + impl.valueOffset_);
}


bool  _aaDelNode(AA aa,...)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
		aaPtr e = impl.getNode(cast(void *)(&aa + 1),NodeOp.op_del);
		if (e !is null)
		{
            impl.freeNode(e);
		    return true;
		}
	}
	return false;
}

/// Find a value, and if it exists, delete and pass the value back, return true
/// If it doesn't exist, return false
///for this to work, caller must pass key value and also a reference to a value buffer.

bool  _aaDelGetValue(AA aa, ...)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
        void* pkey = cast(void *)(&aa + 1);

		aaPtr e = impl.getNode(cast(void *)(&aa + 1),NodeOp.op_del);

		if (e !is null)
		{
		    void** pvalue = cast(void**)(pkey + impl.keySize_);
		    memcpy(*pvalue, cast(void*)(e+1) + impl.valueOffset_, impl.valueSize_);
            impl.freeNode(e);
		    return true;
		}
	}
	return false;
}
/// push the aa, and key, and get a pointer to the value, or null
bool _aaContains(AA aa, ...)
{
    BB* impl = aa.a;
    if (impl !is null)
    {
        return (impl.getNode(cast(void *)(&aa + 1), NodeOp.op_get) !is null);
    }
    return false;
}

/// push the aa, and key, and get a pointer to the value, or null
void* _aaGetNodeValue(AA aa, ...)
{
    BB* impl = aa.a;
    if (impl !is null)
    {
        void* pkey = cast(void*)(&aa + 1);
        aaPtr e = void;
		hash_t key_hash = void;

        immutable maskoff = impl.keyMask_;
		if (maskoff)
		{
			key_hash = *(cast(hash_t*)pkey) & maskoff;
			e = impl.b[key_hash % $];

			while(e != null)
			{
				if (key_hash == e.hash) // keys match exactly!
				{
					break;
				}
				e = e.next;
			}
		}
		else {
		    TypeInfo keyti = impl.keyti;
			key_hash =  keyti.getHash(pkey);
			e = impl.b[key_hash % $];
			while(e != null)
			{
				if ((key_hash == e.hash) && (keyti.compare(pkey, e+1)==0))
                {
                    break;
                }
				e = e.next;
			}
		}
        return (e is null ? null : cast(void *)(e + 1) + impl.valueOffset_);
    }
    return null;
}
/**

**/

/*************************************************
 * Get pointer to value in associative array indexed by key.
 * Returns null if it is not already there.
 */

// we are not making a new BB here , so no need to pass keyti or valuesize

void* _aaGetRvalue(AA aa, TypeInfo keyti, size_t valuesize, ...)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
		aaPtr e = impl.getNode(cast(void *)(&valuesize + 1),NodeOp.op_get);
		return (e is null ? null : cast(void *)(e + 1) + impl.valueOffset_);
    }
    return null;
}


/*************************************************
 * Determine if key is in aa.
 * Returns:
 *      null    not in aa
 *      !=null  in aa, return pointer to value
 */
// we are not making a new BB here, no need to pass the keyti.

void* _aaIn(AA aa, TypeInfo keyti, ...)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
		aaPtr e = impl.getNode(cast(void *)(&keyti + 1), NodeOp.op_get);
		return (e is null ? null : cast(void *)(e + 1) + impl.valueOffset_);
	}
	return null;
}

/*************************************************
 * Delete key entry in aa[].
 * If key is not in aa[], do nothing.
 */
// we are not making a new BB here, no need to pass the keyti.
void _aaDel(AA aa, TypeInfo keyti, ...)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
		impl.getNode(cast(void *)(&keyti + 1),NodeOp.op_del);
	}
}


/********************************************
 * Produce array of values from aa.
 */
// we are not making a new BB here, and we already know the keysize and value size
ArrayRet_t _aaValues(AA aa, size_t keysize, size_t valuesize)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
		return impl.values(null);
	}
	else {
		 Array a;
		 return *cast(ArrayRet_t*)(&a);
	}
}

/********************************************
 * Produce array of N byte keys from aa.
 */
// we are not making a new BB here, and we already know the keysize and value size
ArrayRet_t _aaKeys(AA aa, size_t keysize)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
		return impl.keys();
	}
	else {
		 Array a;
		 return *cast(ArrayRet_t*)(&a);
	}
}


/********************************************
 * Produce array of values from aa.
 */

ArrayRet_t _aaGetValues(AA aa, TypeInfo valti)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
		return impl.values(valti);
	}
	else {
		 Array a;
		 return *cast(ArrayRet_t*)(&a);
	}
}

/********************************************
 * Produce array of N byte keys from aa.
 */

ArrayRet_t _aaGetKeys(AA aa)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
		return impl.keys();
	}
	else {
		 Array a;
		 return *cast(ArrayRet_t*)(&a);
	}
}

size_t
_aaGetKeyValues(AA aa, ArrayRet_t* ka, ArrayRet_t* va, TypeInfo valti)
{
	BB* impl = aa.a;
    if (impl !is null)
    {
        return impl.keysAndValues(ka, va, valti);
    }
    else
    {
        Array a;
        *va = *cast(ArrayRet_t*)(&a);
        *ka = *cast(ArrayRet_t*)(&a);
        return 0;
    }
}

void _aaResize(AA paa)
{
	BB* impl = paa.a;
	if (impl !is null)
		impl.rehash();
}

/********************************************
 * Rehash an array.  This is advertised as a performance enhancement, and should help if table size increases.
 * Trying not to invalidate other references to the original by returning the same rehashed original
 * There must be an easier way to invalidate other references to this AA, eg shallow copy the structure, re-init the original.
 * See _aaUnique
 */

void* _aaRehash(AA* paa, TypeInfo keyti)
{
	BB* impl = (*paa).a;
	if (impl !is null)
		impl.rehash();
    return cast(void*) impl;
}

/**
	Make a new BB, and swap all the original fields.
	The original becomes empty, return the new copy of original
	Aliased references to original object now have empty array
*/
void* _aaAssumeUnique(AA* paa)
{
	BB* impl = paa.a;
	if (impl !is null)
	{
		BB* result = new BB();
		result.swipe(impl);
		return cast(void*) result;
	}
	return null;
}

int _aaApplyKeys(AA aa, dg_t dg)
{
	BB* impl = aa.a;
    if (impl !is null)
		return impl.applyKeys(dg);
	return 0;
}
int _aaApplyTwo(AA aa,dg2_t dg)
{
	BB* impl = aa.a;
    if (impl !is null)
		return impl.applyKeyValues(dg);
	return 0;
}

int _aaApplyOne(AA aa, dg_t dg)
{
	BB* impl = aa.a;
    if (impl !is null)
		return impl.applyValues(dg);
	return 0;
}

/**********************************************
 * 'apply' for associative arrays - to support foreach
 */

 // do not need the key size
int _aaApply(AA aa, size_t keysize, dg_t dg)
in
{
    assert(aligntsize(keysize) == keysize);
}
body
{
	BB* impl = aa.a;
    if (impl !is null)
		return impl.applyValues(dg);
	return 0;
}

 // do not need the key size
int _aaApply2(AA aa, size_t keysize, dg2_t dg)
in
{
    assert(aligntsize(keysize) == keysize);
}
body
{
	BB* impl = aa.a;
    if (impl !is null)
		return impl.applyKeyValues(dg);
	return 0;
}

ArrayRet_t _aaStats(AA aa)
{
	uint[] result;

	if (aa.a)
	{
        version (TEST_BED) if (aa.a.debug_)
            printf("valueOffset = %d\n", aa.a.valueOffset_);
		result = aa.a.statistics();
	}
	else {
		result = new uint[2];
	}

    /*Array a;
    a.ptr = result.ptr;
    a.length = result.length;*/

   return *cast(ArrayRet_t*)(&result);

}
void  _aaDebug(AA aa, bool value)
{
    version(TEST_BED)
    {
    BB* impl  = aa.a;
    if (impl)
        impl.debug_ = value;
    }

}
void  _aaClear(AA aa)
{
	BB* impl  = aa.a;

	if (impl)
    {
        impl.clear;
	}
}

BB* _aaInitCopySetup(AA* target, AA source)
{
    BB* src = source.a;
    if (src is null)
        return null;

    BB* impl = new BB();
    target.a = impl;
    impl.initBB(src.keyti, src.valueSize_, 0, (src.valueOffset_ > 0));
    impl.loadRatio_ = src.loadRatio_;
    version(OPTIONAL_BLOCK_HEAP)
    {
        if (src.heap_ !is null)
            impl.capacity(src.nodes);
    }
    return impl;

}
BB* _aaInitAA(AA* aa, TypeInfo_AssociativeArray ti, bool hashSmall)
{

    auto valuesize = ti.next.tsize();           // value size
    auto keyti = ti.key;
    BB* impl = new BB();
    aa.a = impl;
    impl.initBB(keyti, valuesize, 0, hashSmall);
    return impl;
}

BB* _aaInitHashSet(AA* aa, TypeInfo keyti, bool hashSmall, uint preAllocate)
{
    BB* impl = new BB();
    aa.a = impl;
    impl.initBB(keyti, 0, preAllocate, hashSmall);
    return impl;
}

BB* _aaInitKey(TypeInfo keyti, size_t valuesize, bool hashSmall, uint preAllocate)
{
    BB* impl = new BB();
    impl.initBB(keyti, valuesize, preAllocate, hashSmall);
    return impl;
}

extern (C) void*
_d_assocarrayliteralT(TypeInfo_AssociativeArray ti, size_t length, ...)
{

    auto valuesize = ti.next.tsize();           // value size
    auto keyti = ti.key;
    auto keysize = keyti.tsize();               // key size

    if (length == 0 || valuesize == 0 || keysize == 0)
    {
        return null;
    }
    else
    {
        va_list q;
        va_start!(size_t)(q, length);

        AA  aa;
		BB* impl = _aaInitAA(&aa, ti, true);
		impl.loadRatio = 1.0;
		impl.capacity = length;

        size_t keystacksize   = (keysize   + int.sizeof - 1) & ~(int.sizeof - 1);
        size_t valuestacksize = (valuesize + int.sizeof - 1) & ~(int.sizeof - 1);

        size_t keytsize = aligntsize(keysize);

        for (size_t j = 0; j < length; j++)
        {   void* pkey = q;
            q += keystacksize;
            void* pvalue = q;
            q += valuestacksize;

			aaPtr e = impl.getNode(pkey, NodeOp.op_put);

            void* v_entry = cast(void *)(e + 1) + impl.valueOffset_;

            memcpy(v_entry, pvalue, valuesize);
        }

        va_end(q);

		return cast(void*)impl;
    }

}

double _aaGetLoadRatio(AA aa)
{
    BB* impl = aa.a;
    if (impl !is null)
    {
        return impl.loadRatio_;
    }
    return 0.0; // or nan? or none?
}

void _aaSetLoadRatio(AA aa, double ratio)
{
    BB* impl = aa.a;
    if (impl !is null)
    {
        impl.loadRatio(ratio);
    }
}


size_t _aaGetCapacity(AA aa)
{
    BB* impl = aa.a;
    if (impl !is null)
    {
        return impl.capacity_;
    }
    return 0;
}

void _aaSetCapacity(AA aa, size_t cap)
{
    BB* impl = aa.a;
    if (impl !is null)
    {

        impl.capacity(cap);
    }
}

/***********************************
 * Compare AA contents for equality.
 * Returns:
 *	1	equal
 *	0	not equal
 */
int _aaEqual(TypeInfo_AssociativeArray ti, AA e1, AA e2)
{
    //printf("_aaEqual()\n");
    //printf("keyti = %.*s\n", ti.key.classinfo.name);
    //printf("valueti = %.*s\n", ti.next.classinfo.name);
	BB* c1 = e1.a;
	BB* c2 = e2.a;

    if (c1 is c2)
		return 1;
	if (c1 is null || c2 is null)
		return 0;

	return c1.dataMatch(c2,ti);

}
