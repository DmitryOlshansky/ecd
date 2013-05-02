module hash.hashptr;

import core.memory;
import core.exception;
import std.traits;
import core.stdc.string : memset, memcpy;
import hash.util;
import std.stdio;

//version = NodeHeap;

version = miss_stats;
version = prime_size;

version(NodeHeap)
{
    import tangy.util.container.Container;
    pragma(msg, "version is NodeHeap");
}


/**
 * Copyright: Copyright Digital Mars 2000 - 2010.
 * License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Walter Bright, Sean Kelly

A simple node pointer table + random linear congruential generator / or hashed peturb scheme.

struct aaA
{
    hash_t hash;
    // key ,value , not declared
}


This implementation will make the Key and Value fields explictly typed.

The other feature of builtin AA is the hash and comparison use the TypeInfo of the Key to generate
the hash.

Because the struct contains no explicit pointers, might not be scanned by the GC.


*/


class AAKeyError : Error {
	this(string msg)
	{
		super(msg);
	}
}

/// The node structure for the hash table trees.
private struct Entry(K,V)
{

    static if (K.sizeof > size_t.sizeof || is (K == class))
        hash_t hash_;

    K      key_;
    V      value_;
}

private import std.c.stdlib;


struct aahp(K,V, bool useRandom = true)
{
    alias HashPtr!(K,V,useRandom)  AAClassImpl;
    alias Entry!(K,V)       Node;
    alias Node*             NodePtr;

    AAClassImpl  imp_;

	V* opIn_r(K k)
	{
		//wrap
		if (imp_ is null)
            return null;
        NodePtr* np = imp_.getNode(k);
        if (np is null)
            return null;
        return &(*np).value_;
	}

    V opIndex(K k)
    {
        if (imp_ !is null)
        {
            NodePtr* np = imp_.getNode(k);

            if (np !is null)
                return (*np).value_;
        }
        throw new AAKeyError("no key for opIndex");
    }
	void opIndexAssign(V value, K k)
	{
	    if (imp_ is null)
            imp_ = new AAClassImpl();
        imp_.assign(value, k);
	}

    void detach()
    {
        imp_ = null;
    }
    void clear()
    {
        if (imp_ !is null)
            imp_.clear();

    }

    @property void capacity(size_t cap)
    {
 	    if (imp_ is null)
            imp_ = new AAClassImpl();
        imp_.capacity(cap);
    }

    @property size_t capacity() {
	    if (imp_ is null)
            return 0;
        return imp_.capacity;
    }
    @property void rehash()
    {
	    if (imp_ !is null)
            imp_.rehash;
    }

    @property
	K[] keys()
	{
	    if (imp_ is null)
            return null;
        return imp_.keys();
	}
    @property aahp allocate()
    {
        aahp result;
        result.imp_ = new AAClassImpl();
        return result;
    }

    @property aahp init()
    {
        aahp result;
        return result;
    }

    @property
	V[] values()
	{
	    if (imp_ is null)
            return null;
        return imp_.values();
	}

    V get(K k)
    {
		V* p = opIn_r(k);
		if (p !is null)
		{
			return *p;
		}
		return V.init;
    }

    @property void loadRatio(double ratio)
    {
        // dummy
    }
    @property double loadRatio()
    {
        return 0.6;
    }
	bool get(K k, ref V val )
	{
	    if (imp_ !is null)
	    {
            V* p = opIn_r(k);
            if (p !is null)
            {
                val = *p;
                return true;
            }
	    }
		val = V.init;
		return false;
	}
    version(miss_stats)
    {
        @property uint[] list_stats()
        {
            if (imp_ is null)
                return null;
            //'return imp_.used;
            return imp_.list_stats;
        }

        @property size_t rehash_ct()
        {
            if (imp_ is null)
                return 0;
            //'return imp_.used;
            return imp_.rehash_ct;
        }

        @property size_t misses()
        {
            if (imp_ is null)
                return 0;
            //'return imp_.used;
            return imp_.misses;
        }
    }

	@property size_t length()
	{
	    if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.nodes_;
	}


    @property aahp dup()
    {
        aahp copy;

        if (imp_ !is null)
        {
            copy.imp_ = new AAClassImpl(imp_);
        }
        return copy;
    }

    void remove(K key)
    {
        if (imp_ !is null)
            imp_.remove(key);
    }
	public int opApply(int delegate(ref V value) dg)
	{
		return (imp_ !is null) ? imp_.opApply(dg) : 0;
	}

	public int opApply(int delegate(ref K key, ref V value) dg)
	{
        return (imp_ !is null) ? imp_.opApply(dg) : 0;
	}
}

/// implementation as a class


class HashPtr(K, V,  bool useRandom = true)
{
private:
    alias typeof(this)   SelfType;

	alias Entry!(K,V)  Node;
    alias Node*        NodePtr;

    enum : size_t  {lc_mul  = 1103515245U, lc_add = 12345U}
    enum : size_t  {PERTURB_SHIFT = 32}
    enum : real    {FULL_CAPACITY = 0.6}
    enum : size_t  { ptr_vacant = 1}     // empty, but was in a lookup sequence, therefore cannot abort a search
version(NodeHeap)
{
    private alias Container.DefaultCollect Heap;
    private alias Heap!(Node) Alloc;
    private Alloc heap;

    ~this()
    {
        heap.collect(true);
    }
}

	NodePtr[]    table_;
    size_t      nodes_;
    size_t      capacity_;  // how many nodes before next rehash
    size_t      mask_;
    // TODO: replace this with TypeInfo_AssociativeArray when available in _aaGet()
    TypeInfo    keyti_;

    version(miss_stats)
    {
         size_t      max_misses_;
         size_t      rehash_ct_;
    }



	public this(size_t cap = 0)
	{
		keyti_ = typeid(K);
		if (cap > 0)
		{
		    this.capacity(cap);
		}
	}

    public this(SelfType copyme)
    {
        foreach(k,v ; copyme)
            this.opIndexAssign(v, k);
    }

    @property final size_t length()
    {
         return nodes_;
    }
    // randomizes lower bits
    private static hash_t overhash(hash_t h)
    {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }
    /// 0: table length, 1 : unoccupied nodes,
    /// 2* : followed by [n-2] :  number of nodes on bucket n
    @property uint[] list_stats()
    {
        uint[] result = new uint[2];

        result[0] = table_.length;
        result[1] = table_.length - nodes_;

        return result;
    }

    version (miss_stats)
    {
        @property size_t misses()
        {
             return max_misses_;
        }
        @property size_t rehash_ct()
        {
             return rehash_ct_;
        }    }

debug {
    @property public size_t footprint() { return nodes_ * Entry.sizeof + table_.length * NodePtr.sizeof;}
    @property public size_t nodecount() { return nodes_; }
}
    @property public size_t length(){ 	return nodes_; } // not sure yet


    /// return a node pointer. Null if not found, or cannot create
    /// if create was true, set it to false if the node was not created
    private static void freeTable(ref NodePtr[] ntable)
    {

        delete ntable;
        ntable = null;

    }

    /** Return a pointer to a existing node, or null, if create is false.
    Return a pointer to existing node, or create a new node with the key, if create is true.
    */
	private final NodePtr* getNode(K key, bool create = false)
	{
	    if (table_ is null)
	    {
	        if (create)
	        {

	            version(prime_size)
	            {
	                size_t nlen = getNextPrime(1);
	            }

                else {
                    nlen = 128;
                    mask_ = 127;
	            }
	            table_ = new NodePtr[nlen];
	            capacity_ = cast(size_t)(table_.length * FULL_CAPACITY);     // capacity tolerance
	        }

            else
                return null;
	    }


        immutable table_size = table_.length;

        static if (K.sizeof > size_t.sizeof || is (K == class))
        {
            hash_t key_hash =  keyti_.getHash(&key);
        }
        else {
            hash_t key_hash = overhash(key);
        }
        size_t my_chair = key_hash;


        version(prime_size)
            size_t nix = (my_chair % table_size);
        else
            size_t nix = (my_chair & mask_);

        NodePtr* pe = &table_[nix];


        static if (!useRandom)
        {
            size_t perturb = key_hash;
        }
        size_t misses = 0;

        /// hop skip and jump
        NodePtr* first_vacant = null;
        for(;;)
        {
            // check for empty

            NodePtr  e = *pe;

                //check for empty
            if (e is null)
            {
                 if (first_vacant !is null)
                    pe = first_vacant;
                 goto EMPTY_SLOT; // will have to return a new node (create), or null
                // check for exact match
            }


            if (e == cast(NodePtr) ptr_vacant && first_vacant is null)
            {
                // formerly occupied in lookup sequence
                first_vacant = pe;

            }
            else {
                // occupied node, see if hash / key match
                static if (K.sizeof > size_t.sizeof || is (K == class))
                {
                    if ((key_hash == e.hash_) && (keyti_.compare(&key, &e.key_)==0))
                    {
                        return pe;
                    }

                }
                else {
                    if (key == e.key_)
                    {
                        return pe;
                    }

                }
            }
            // else look elsewhere

            version(miss_stats)
            {
                misses++;
                if (misses > max_misses_)
                    max_misses_ = misses;
            }
            static if(useRandom)
            {
                my_chair = my_chair * lc_mul + lc_add; // next in a sequence that started with the hash
            }
            else {
                my_chair = (my_chair*5 + perturb + 1);
                perturb /= PERTURB_SHIFT;
            }
            version(prime_size)
                nix = (my_chair % table_size); // address of array element
            else
                nix =  (my_chair & mask_); // address of array element
            pe = &table_[nix];

        }
    EMPTY_SLOT:
        /// pe points to null
        if (create)
        {

            version(NodeHeap)
                NodePtr e2 = heap.allocate();
            else
                NodePtr e2 = new Node();

            e2.key_ = key;

            static if (K.sizeof > size_t.sizeof || is(K == class))
                e2.hash_ = key_hash;
            *pe = e2;
            nodes_++;
            return pe;
        }
        return null; // not found and not create
	}

    // save some rehashing by specifying the expected maximum of entries
    @property public size_t capacity() { return capacity_; }

    @property public final void capacity(size_t cap)
    {
        size_t nlen = cast(size_t)(cap / FULL_CAPACITY);
        version (prime_size)
            nlen = getNextPrime(nlen);

        if (nlen > table_.length)
        {
            resizeTable(nlen);
        }
    }
	public final V* opIn_r(K k)
	{
        NodePtr* pe = getNode(k);
        if (pe is null)
            return null;
        return &(*pe).value_;
	}

    public final V get(K k)
    {
        NodePtr* pe = getNode(k);
        if (pe is null)
            throw new AAKeyError("get on absent AA key");
        return (*pe).value_;
    }

    public final bool get(K k, ref V val )
	{
		NodePtr* pe = getNode(k);
		if (pe is null)
        {
            val = V.init;
            return false;
        }
        val = (*pe).value_;
        return true;
	}

    public final V opIndex(K k)
	{
		NodePtr* pe = getNode(k);
		if (pe is null)
        {
            throw new AAKeyError("index on absent AA key");
        }
        return (*pe).value_;
	}

     private void assign(ref V value, K k)
     {

		NodePtr* pe = getNode(k, true);
        (*pe).value_ = value;

        if (nodes_ > capacity_)
        {
            rehash();
        }
     }
	 public void opIndexAssign(V value, K k)
	 {
         assign(value,k);
	 }

     public bool remove(K k)
     {
        NodePtr* pe = getNode(k);
        if (pe is null)
        {
            return false;
        }

        NodePtr e = *pe;
        *pe = cast(NodePtr)ptr_vacant; // may have broken a lookup sequence

        version(NodeHeap)
        {
            heap.collect(e);
        }
        else {
            delete e;
        }
        --nodes_;
        return true;
    }
    private void resizeTable(size_t nlen)
    {
        NodePtr[] newtable;
        version(prime_size)
        {
            newtable = new NodePtr[nlen];
        }
        else {
            newtable = new NodePtr[nlen];
            mask_ = nlen - 1;
        }
        immutable size_t new_length = newtable.length;

        foreach (e; table_)
        {
            if (e !is null && (e != cast(NodePtr) ptr_vacant))
            {
                static if (K.sizeof > size_t.sizeof  || is (K == class))
                {
                    auto key_hash = e.hash_;
                }
                else {
                    hash_t key_hash = e.key_;
                }
                size_t my_chair = key_hash;

                version(prime_size)
                    size_t nix = (my_chair % new_length);
                else
                    size_t nix = (my_chair & mask_);

                static if (!useRandom)
                {
                    size_t perturb = key_hash;
                }


                for(;;)
                {
                    NodePtr* pe = &newtable[nix];

                    if (*pe is null)
                    {
                        *pe = e;
                        break;
                    }

                    static if (useRandom)
                    {
                         my_chair = my_chair * lc_mul + lc_add;
                    }
                    else {
                        my_chair = (my_chair*5 + perturb + 1);
                        perturb /= PERTURB_SHIFT;
                    }
                    version(prime_size)
                        nix = (my_chair % new_length);
                    else
                        nix = (my_chair & mask_);

               }

            }
        }
        freeTable(table_);
        table_ = newtable;
        capacity_ = cast(size_t)(table_.length * FULL_CAPACITY);
   }


    private void rehash()
    {
        // get a new table_



        if (nodes_)
        {
            size_t ix;

            version(prime_size)
            {
                size_t nlen = getNextPrime(nodes_*2); // looking to always get a big table size jump
            }

            else {
                size_t nlen = table_.length*2; // looking to always get a big table size jump
            }

            version (miss_stats)
            {
                rehash_ct_++;

                /*if (table_.length == nlen)
                {
                    throw new AAKeyError("same rehash size");
                }*/
            }

            resizeTable(nlen);
        }
        else {
            // unlikely?
            freeTable(table_);
        }
    }


    private void deleteAllNodes()
    {

        foreach(ref e ; table_)
        {
            if (e !is null)
            {
                version(NodeHeap)
                {
                heap.collect(e);
                }
                else {
                    delete e;
                }
                e = null;
            }
        }
        freeTable(table_);
        nodes_ = 0;
    }

    private void clean()
    {
        if (table_ is null)
            return;

        version(NodeHeap)
        {
            heap.clear();
            freeTable(table_);
            nodes_ = 0;
        }
        else
        {
            deleteAllNodes();
        }


    }

    public void clear()
    {
        clean();
    }

	/*
	* We use this template to cast a static array to a dynamic one in opApply,
	* since the dmd specs don't allow them as ref parameters :F
	*/
	private template DeconstArrayType(T)
	{
		static if(isStaticArray!(T))
		{
			alias typeof(T.init[0])[] type; //the equivalent dynamic array
		}
		else
		{
			alias T type;
		}
	}

	alias DeconstArrayType!(K).type K_;
	alias DeconstArrayType!(V).type V_;

	public int opApply(int delegate(ref V_ value) dg)
	{
		return opApply((ref K_ k, ref V_ v) { return dg(v); });
	}

	public int opApply(int delegate(ref K_ key, ref V_ value) dg)
	{

        int result;
        foreach (e;table_)
        {
            if (e !is null)
            {
                result = dg(e.key_, e.value_);
                if (result || nodes_ == 0)
                    break;
            }
        }
        return result;
	}


	public K[] keys()
	{
		K[] keyBlock = new K[](this.nodes_);
		size_t ix = 0;

        if(keyBlock.length > 0)
        {

            foreach(e ; table_)
            {
                if (e !is null)
                {
                    keyBlock[ix++] = e.key_;
                }
            }
        }
		return keyBlock;
	}

	public V[] values()
	{
		V[] valueBlock = new V[](this.nodes_);
		size_t ix = 0;


        if(valueBlock.length > 0)
        {
            foreach(e ; table_)
            {
                if (e !is null)
                {
                    valueBlock[ix++] = e.value_;
                }
            }
        }

		return valueBlock;
	}


}


template isReferenceType(T) {
    enum isReferenceType = is(T : const(void*))  ||
                           is( T == class )     ||
                           is( T == interface ) ||
                           is( T == delegate );
 }


void unit_test()
{
	//
	aahp!(int, int) aa;
	int test;
	int* ptest;
	// test unused key assign
	aa[0] = 1;

	// test dummy key assign
	aa[1] = 2;


	assert(aa.get(0,test));
	assert(aa.get(1,test));

	// test missing key

	string msg = "test missing key";

	assert (!aa.get(100,test),msg);

	try {
		test = aa[100];
		assert(0,"no missing key exception");
	}
	catch(AAKeyError e)
	{
		// ok
	}

	aa[100] = 3;
	ptest = 100 in aa;
	assert(ptest !is null);

    auto keys = aa.keys;
	assert(keys[0..3] == [0,1,100]);
	auto values = aa.values;
	assert(values[0..3] == [1,2,3]);

    auto bb = aa.dup;
    values = bb.values;
    keys = bb.keys;
    assert(values[0..3] == [1,2,3]);
	assert(keys[0..3] == [0,1,100]);

	bb.clear;

	assert(aa.length == 3);
}
