module hash.hashlist;

import hash.util;
import core.memory;
import core.exception;
import std.traits;
import core.stdc.string : memset, memcpy;

import std.stdio;

//version = NodeHeap;
version = miss_stats;
version = prime_size;

version(NodeHeap)
{
    import tangy.util.container.Container;
    pragma(msg, "hashlist uses NodeHeap");
}
private {
    extern (C) void* gc_malloc( size_t sz, uint ba = 0 );
    extern (C) void* gc_calloc( size_t sz, uint ba = 0 );
    extern (C) void  gc_free( void* p );
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
    alias Entry*    EntryPtr;
    static if (K.sizeof > size_t.sizeof || is(K==class))
        hash_t hash_;
    EntryPtr next_;
    K      key_;
    V      value_;
}

private import std.c.stdlib;


struct aahl(K,V)
{
    alias HashList!(K,V)  AAClassImpl;
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
            {
                return (*np).value_;
            }
        }
        throw new AAKeyError("no key for opIndex");
    }
	void opIndexAssign(V value, K k)
	{
	    if (imp_ is null)
            imp_ = new AAClassImpl();
        imp_.assign(value, k);
	}

    @property aahl allocate()
    {
        aahl result;
        result.imp_ = new AAClassImpl();
        return result;
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

    @property
	K[] keys()
	{
	    if (imp_ is null)
            return null;
        return imp_.keys();
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
    @property double loadRatio()
    {
        if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.loadRatio;
    }
    @property aahl assumeUnique()
    {
        aahl result;
        if (imp_)
        {
             result.imp_ =  new AAClassImpl();
             imp_.swipe(imp_);
        }
        return result;
    }
    @property void loadRatio(double ratio)
    {
	    if (imp_ is null)
            imp_ = new AAClassImpl();
        imp_.loadRatio(ratio);
    }
    @property size_t capacity()
    {
        if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.capacity;
    }

    @property void capacity(size_t cap)
    {
	    if (imp_ is null)
            imp_ = new AAClassImpl();
        imp_.capacity(cap);
    }
    version(miss_stats)
    {


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
        @property uint[] list_stats()
        {
            if (imp_ is null)
                return null;
            //'return imp_.used;
            return imp_.list_stats;
        }
    }

	@property size_t length()
	{
	    if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.nodes_;
	}



    @property aahl dup()
    {
        aahl copy;

        if (imp_ !is null)
        {
            copy.imp_ = new AAClassImpl(imp_);
        }
        return copy;
    }
    @property void rehash()
    {
	    if (imp_ !is null)
            imp_.rehash;
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

    /// return with having the only references to data table and nodes

/// implementation as a class


class HashList(K, V)
{
private:
    alias typeof(this)   SelfType;

private {
	alias Entry!(K,V)  Node;
    alias Node*        NodePtr;
}


    enum : real { LOAD_FACTOR = 3}

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

    private {

        NodePtr[]     table_;
        size_t      nodes_;
        size_t      capacity_;  // how many nodes before next rehash
        //size_t      mask_;
        // TODO: replace this with TypeInfo_AssociativeArray when available in _aaGet()
        TypeInfo    keyti_;
        double     load_factor_;
    }

    version(miss_stats)
    {
         size_t      max_misses_;
         size_t      rehash_ct_;
    }


    private void forget()
    {
        table_ = null;
        nodes_ = 0;
        capacity_ = 0;
        version(NodeHeap)
            heap = heap.init;
    }
    /// move everything from other reference to this.
    /// Everything already in this reference is forgotten,
    /// so best used with new reference
    private void swipe(HashList other)
    {
        table_ = other.table_;
        nodes_ = other.nodes_;
        capacity_ = other.capacity_;
        load_factor_ = other.load_factor_;
        keyti_ = other.keyti_;

        version(NodeHeap)
            heap = other.heap;
        other.forget();
    }

	public this(size_t cap = 0)
	{
		keyti_ = typeid(K);
		load_factor_ = 1.0;
		if (cap > 0)
		{
		    capacity(cap);
		}

	}

    public this(SelfType copyme)
    {
        keyti_ = typeid(K);
        load_factor_ = copyme.load_factor_;
        capacity(copyme.capacity);
        foreach(k,v ; copyme)
            this.opIndexAssign(v, k);
    }


    @property void capacity(size_t cap)
    {
        size_t nlen = cast(size_t)(cap / load_factor_);
        nlen = getNextPrime(nlen);
        if (nlen > table_.length)
            resizeTable(nlen);

    }
    @property size_t capacity()
    {
        return capacity_;
    }
    /// allows ratio to vary between 1 and 10, default 3
    @property void loadRatio(double ratio)
    {
        if (ratio < 0.5)
            ratio = 0.5;
        else if (ratio > 16.0)
            ratio = 16.0;
        load_factor_ = ratio;
    }
    @property double loadRatio()
    {
        return load_factor_;
    }

    @property final size_t length()
    {
         return nodes_;
    }

    private static hash_t overhash(hash_t h)
    {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
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
        }

        /// 0: table length, 1 : unoccupied nodes,
        /// 2* : followed by [n-2] :  number of nodes on bucket n
        @property uint[] list_stats()
        {
            uint result[];
            uint emptyCt = 0;

            result.length = 16;

            result[0] = table_.length;

            foreach(e ; table_)
            {
                if(e !is null)
                {
                    uint listct = 0;
                    while (e !is null)
                    {
                        listct++;
                        e = e.next_;
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
    }

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

    private static NodePtr[] allocTable(size_t newsize)
    {
        return new NodePtr[newsize];
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
	            //
	            version(prime_size)
                    size_t nlen = getNextPrime(1);
                else {
                    size_t nlen = 128;
                    mask_ = 127;
	            }
	            table_ = new NodePtr[nlen];
	            capacity_ = cast(size_t)(table_.length * load_factor_);     // capacity tolerance
	        }

            else
                return null;
	    }


        immutable table_size = table_.length;

        static if (K.sizeof > size_t.sizeof || is(K==class))
        {
            hash_t key_hash =  keyti_.getHash(&key);
        }
        else {
            hash_t key_hash = key;
        }
        size_t my_chair = key_hash;


        version(prime_size)
            size_t nix = (my_chair % table_size);
        else
            size_t nix = (my_chair & mask_);

        NodePtr* pe = &table_[nix];

        version(miss_stats)
            size_t misses = 0;

        for(;;)
        {
            // check for empty

            NodePtr e = *pe;

                //check for empty
            if (e is null)
                goto EMPTY_SLOT; // will have to return a new node (create), or null
                // check for exact match

            static if (K.sizeof > size_t.sizeof || is(K==class))
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
            version(miss_stats)
            {
               misses++;
               if (misses > max_misses_)
                    max_misses_ =misses;
            }

            pe = &e.next_;
        }
    EMPTY_SLOT:
        /// pe points to null
        if (create)
        {

            version(NodeHeap)
                NodePtr e2 = heap.allocate();
            else
                NodePtr e2 = cast(Node*)gc_calloc(Node.sizeof);


            e2.key_ = key;

            static if (K.sizeof > size_t.sizeof || is(K==class))
                e2.hash_ = key_hash;


            *pe = e2;
            nodes_++;
            return pe;
        }
        return null; // not found and not create
	}

	public final V* opIn_r(K k)
	{
        NodePtr* pe = getNode(k);
        if (pe is null)
            return null;
        NodePtr e = *pe;
        return &e.value_;
	}

    public final V get(K k)
    {
        NodePtr* pe = getNode(k);
        if (pe is null)
            throw new AAKeyError("get on absent AA key");
        NodePtr e = *pe;
        return e.value_;
    }

    public final bool get(K k, ref V val )
	{
		NodePtr* pe = getNode(k);

		if (pe is null)
        {
            val = V.init;
            return false;
        }
        NodePtr e = *pe;
        val = e.value_;
        return true;
	}


    public final V opIndex(K k)
	{
		NodePtr* pe = getNode(k);
		if (pe is null)
        {
            throw new AAKeyError("index on absent AA key");
        }
        NodePtr e = *pe;
        return e.value_;
	}

     private void assign(ref V value, K k)
     {

		NodePtr* pe = getNode(k, true);
		NodePtr e = *pe;
        e.value_ = value;

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
        *pe = e.next_;

        version(NodeHeap)
        {
            heap.collect(e);
        }
        else {
            gc_free(e);
        }
        --nodes_;
        return true;
     }


    private void resizeTable(size_t nlen)
    {
        NodePtr[] newtable = new NodePtr[nlen];
        immutable size_t new_length = newtable.length;

        if (nodes_)
        foreach (e; table_)
        {

            while(e !is null)
            {
                NodePtr next = e.next_;
                e.next_ = null;

                static if (K.sizeof > size_t.sizeof || is(K==class))
                {
                    auto key_hash = e.hash_;
                }
                else {
                    hash_t key_hash = e.key_;
                }
                NodePtr* pe = &newtable[key_hash % new_length];

                while (*pe !is null)
                {
                    pe = &(*pe).next_;
                }
                *pe = e;
                e = next;
           }
        }
        freeTable(table_);
        table_ = newtable;
        capacity_ = cast(size_t)(table_.length * load_factor_);

    }
    private void rehash()
    {
        if (nodes_)
        {
            if (nodes_ > capacity_)
            {
                double variance = 1.5;
                double lower_cap = load_factor_ / variance;
                double upper_cap = load_factor_ * variance;
                //writefln("pre-rehash %d  %5.3f", nodes_, cast(double)nodes_/table_.length);
                size_t nlen = cast(size_t)( nodes_ / lower_cap);
                resizeTable(getNextPrime(nlen));

                capacity_ = cast(size_t)( table_.length * upper_cap);
                //writefln("post-rehash %d  %5.3f capacity %d", nodes_, cast(double)nodes_/table_.length,capacity_);
                return;
            }
        }
        capacity(nodes_);
    }


    private void deleteAllNodes()
    {

        foreach(ref e ; table_)
        {
            while(e !is null)
            {
                NodePtr nx = e.next_;
                version(NodeHeap)
                    heap.collect(e);
                else
                    gc_free(e);
                e = nx;
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
            while (e !is null)
            {
                NodePtr nx = e.next_;

                result = dg(e.key_, e.value_);
                if (result || nodes_ == 0)
                    break;
                e = nx;
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
                while (e !is null)
                {
                    keyBlock[ix++] = e.key_;
                    e = e.next_;
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
                while (e !is null)
                {
                    valueBlock[ix++] = e.value_;
                    e = e.next_;
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
	aahl!(int, int) aa;
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
