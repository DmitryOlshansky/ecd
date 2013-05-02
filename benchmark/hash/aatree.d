module hash.aatree;

import core.memory;
import core.exception;
import std.traits;
import core.stdc.string : memset, memcpy;
import hash.util;
import std.stdio;
import std.conv;

//version = NodeHeap;

version(NodeHeap)
{
    import tangy.util.container.Container;
    pragma(msg, "version is NodeHeap");
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

*  Attempt to understand the AA behaviour by an implementation as a class.
*  Michael Rynn.

Design. As noted in the druntime/src/aA.d, the hash table node implementation is a structure,
with left and right pointers, and a hash key.  Past the end of this fixed sized node memory block is a
 key and value fields.

struct aaA
{
    aaA *left;
    aaA *right;
    hash_t hash;
    // key ,value , not declared
}


This implementation will make the Key and Value fields explictly typed.

The other feature of builtin AA is the hash and comparison use the TypeInfo of the Key to generate
the hash.

Because the struct contains explicit pointers, it will always be scanned by the GC.


*/





/// The node structure for the hash table trees.
private struct Entry(K,V)
{
    Entry* left_;
    Entry* right_;

    static if (K.sizeof > size_t.sizeof || is(K == class))
        hash_t hash_;

    K      key_;
    V      value_;
}

private import std.c.stdlib;


struct aaht(K,V)
{
    alias HashTree!(K,V)  AAClassImpl;
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
        throw new AAKeyError("key not found " ~ to!(string)(k));

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

    @property aaht assumeUnique()
    {
        aaht result;
        if (imp_)
        {
             result.imp_ =  new AAClassImpl();
             imp_.swipe(imp_);
        }
        return result;
    }

    @property aaht allocate()
    {
        aaht result;
        result.imp_ = new AAClassImpl();
        return result;
    }

    @property double loadRatio()
    {
        if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.loadRatio;
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

    @property uint[] list_stats()
    {
        if (imp_ is null)
            return null;
        //'return imp_.used;
        return imp_.list_stats;
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

	@property size_t length()
	{
	    if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.nodes_;
	}


    @property aaht dup()
    {
        aaht copy;

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


class HashTree(K, V)
{
private:
    alias typeof(this)   SelfType;

	alias Entry!(K,V)  Node;
    alias Node*        NodePtr;

    enum : double { DEFAULT_LOAD_RATIO = 2.0}


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
        NodePtr[]    table_;
        size_t      nodes_;
        size_t      capacity_;

        TypeInfo    keyti_;
        double     load_factor_;
    }

    private void forget()
    {
        table_ = null;
        nodes_ = 0;
        capacity_ = 0;
        version(NodeHeap)
            heap = heap.init;
    }
    void swipe(SelfType other)
    {
        this.table_ = other.table_;
        this.nodes_ = other.nodes_;
        this.capacity_ = other.capacity_;
        this.keyti_ = other.keyti_;
        this.load_factor_ = other.load_factor_;
        version(NodeHeap)
            heap = other.heap;
        other.forget();
    }

	public this()
	{
		keyti_ = typeid(K);
		load_factor_ = DEFAULT_LOAD_RATIO;
	}

    public this(SelfType copyme)
    {
        load_factor_ = copyme.load_factor_;
        keyti_ = typeid(K);
        capacity(copyme.nodes_);
        foreach(k,v ; copyme)
            this.opIndexAssign(v, k);
    }

    @property final size_t length()
    in {
    }
    out (result) {
        size_t len = 0;
        void countNodes(Node* ex)
        {
            auto e = ex;
            len++;

            while (1)
            {
                if (e.right_)
                   countNodes(e.right_);
                e = e.left_;
                if (!e)
                    break;
                len++;
            }
        }

        foreach (e; table_)
        {
            if (e)
                countNodes(e);
        }

        assert(len == result);
    }
    body {
        return nodes_;
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

    @property double loadRatio()
    {
        return load_factor_;
    }
    /// Ratio of number of nodes to table.length. Ranges from 1 to 10, default 4
    @property void loadRatio(double ratio)
    {
        if (ratio < 0.5)
            ratio = 0.5;
        else if (ratio > 16.0)
            ratio = 16.0;
        load_factor_ = ratio;
    }

    @property size_t capacity()
    {
        return capacity_;
    }

    @property void capacity(size_t cap)
    {
        size_t nlen = cast(size_t)(cap / load_factor_);

        nlen = getNextPrime(nlen);
        if (nlen > nodes_)
            resizeTable(nlen);
    }
    private static NodePtr[] allocTable(size_t newsize)
    {
        return new NodePtr[newsize];
    }

	private NodePtr* getNode(K key, bool create = false)
	{
	    if (table_ is null)
	    {
	        if (create)
	        {
	            table_ = allocTable(getNextPrime(1));
	            capacity_ = cast(size_t) (table_.length * load_factor_);
	        }

            else
                return null;
	    }


        K* pkey = &key;
        static if ((K.sizeof > size_t.sizeof) || is(K == class))
        {
            hash_t key_hash = keyti_.getHash(pkey);// wants void*
        }
        else {
            hash_t key_hash = key;
        }
        size_t i = key_hash % table_.length;
        auto pe = &table_[i]; // address of array element

        for(;;)
        {
            Node* e = *pe;
            if (e is null)
                break;

            static if (K.sizeof > size_t.sizeof || is(K == class))
            {
                if (key_hash == e.hash_)
                {
                    auto c = keyti_.compare(pkey, &e.key_);
                    if(c == 0)
                    {
                        //create = false; // node exists, no need to create
                        return pe;
                    }
                    pe = (c < 0) ? &e.left_ : &e.right_;
                }
                else {
                    pe = (key_hash < e.hash_) ? &e.left_ : &e.right_;
                }
            }
            else {
                if (key == e.key_)
                {
                        //create = false; // node exists, no need to create
                    return pe;
                }
                hash_t e_hash = e.key_;
                pe = (key_hash < e_hash) ? &e.left_ : &e.right_;
            }
        }
        if (create)
        {
            version(NodeHeap)
                NodePtr e2 = heap.allocate();
            else
                NodePtr e2 = cast(Node*) gc_calloc(Node.sizeof);

            e2.key_ = key;

            static if (K.sizeof > size_t.sizeof || is(K == class))
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
        /// 0: table length, 1 : unoccupied nodes,
        /// 2* : followed by [n-2] :  number of nodes on bucket n
    @property uint[] list_stats()
    {
        uint result[];
        uint emptyCt = 0;

        result.length = 16;

        result[0] = table_.length;


        void child_ct(NodePtr np, ref uint ct)
        {

            if (np.left_ !is null)
            {
                ct++;
                child_ct(np.left_,ct);
            }
            if (np.right_ !is null)
            {
                ct++;
                child_ct(np.right_,ct);
            }
        }
        foreach(e ; table_)
        {
            if(e !is null)
            {
                uint listct = 1;

                child_ct(e, listct);

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

     public void remove(K k)
     {
        NodePtr* pe = getNode(k);
        if (pe is null)
        {
            return;
        }
        NodePtr e = *pe;

        if (!e.left_ && !e.right_)
        {
            *pe = null;
        }
        else if (e.left_ && !e.right_)
        {
            *pe = e.left_;
             e.left_ = null;
        }
        else if (!e.left_ && e.right_)
        {
            *pe = e.right_;
             e.right_ = null;
        }
        else
        {
            *pe = e.left_;
            e.left_ = null;
            do {
                pe = &(*pe).right_;
            }
            while (*pe);
            *pe = e.right_;
            e.right_ = null;
        }
        version(NodeHeap)
        {
            heap.collect(e);
        }
        else {
            gc_free( e);
        }
         --nodes_;
     }


    /// Make a new table, balance the trees
    private void resizeTable(size_t nlen)
    {
        // get a new table_

        NodePtr[] newtable;

        void rehash_x(NodePtr olde)
        {
            while (1)
            {
                auto left = olde.left_;
                auto right = olde.right_;
                olde.left_ = null;
                olde.right_ = null;

                //printf("rehash %p\n", olde);
                static if (K.sizeof > size_t.sizeof || is(K == class))
                {
                    auto key_hash = olde.hash_;
                }
                else {
                    hash_t key_hash = olde.key_;
                }
                size_t i = key_hash % newtable.length;
                auto pe = &newtable[i];
                for(;;)

                {
                    NodePtr e = *pe;
                    if (e is null)
                        break;
                    //printf("\te = %p, e.left = %p, e.right = %p\n", e, e.left, e.right);
                    assert(e.left_ != e);
                    assert(e.right_ != e);

                    static if (K.sizeof > size_t.sizeof || is(K == class))
                    {
                        if (key_hash == e.hash_)
                        {
                            auto c = keyti_.compare(&olde.key_, &e.key_);
                            assert(c != 0);
                            pe = (c < 0) ? &e.left_ : &e.right_;
                        }
                        else
                            pe = (key_hash < e.hash_) ? &e.left_ : &e.right_;
                    }
                    else {
                        hash_t hash_new = e.key_;
                        assert (hash_new != key_hash);
                        pe = (key_hash < hash_new) ? &e.left_ : &e.right_;
                    }
                 }
                *pe = olde;

                if (right)
                {
                    if (!left)
                    {   olde = right;
                        continue;
                    }
                    rehash_x(right);
                }
                if (!left)
                    break;
                olde = left;
            }
        }

        newtable = allocTable(nlen);
        foreach (e; table_)
        {
            if (e)
                rehash_x(e);
        }

        freeTable(table_);
        table_ = newtable;
        capacity_ = cast(size_t) (table_.length * load_factor_);

        balance();

    }

    void rehash()
    {
        if (nodes_ > capacity_)
        {
            // growing
            capacity(nodes_ * 4);
        }
    }

    private void balance()
    {

        NodePtr[16] tmp;
        NodePtr[] array = tmp;

        foreach (j, e; table_)
        {
            /* Temporarily store contents of bucket in array[]
             */
            size_t k = 0;
            void addToArray(NodePtr e)
            {
                while (e)
                {   addToArray(e.left_);
                    if (k == array.length)
                        array.length = array.length * 2;
                    array[k++] = e;
                    e = e.right_;
                }
            }
            addToArray(e);
            /* The contents of the bucket are now sorted into array[].
             * Rebuild the tree.
             */
            void buildTree(NodePtr* p, size_t x1, size_t x2)
            {
                if (x1 >= x2)
                    *p = null;
                else
                {   auto mid = (x1 + x2) >> 1;
                    *p = array[mid];
                    buildTree(&(*p).left_, x1, mid);
                    buildTree(&(*p).right_, mid + 1, x2);
                }
            }
            auto p = &table_[j];
            buildTree(p, 0, k);
        }
    }



    private void deleteAllNodes()
    {
        void del_tree(NodePtr e)
        {
            auto left = e.left_;
            auto right = e.right_;
            if (left !is null)
            {
                del_tree(left);
            }
            if (right !is null)
            {
                del_tree(right);
            }
            version(NodeHeap)
            {
                heap.collect(e);
            }
            else {
                gc_free( e);
            }
        }

        foreach(ref e ; table_)
        {
            if (e !is null)
            {
                del_tree(e);
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
        int treewalker(NodePtr e)
        {
            int result;

            do
            {
                //printf("treewalker(e = %p, dg = x%llx)\n", e, dg);
                result = dg(e.key_, e.value_);
                if (result || nodes_ == 0)
                    break;
                if (e.right_)
                {   if (!e.left_)
                    {
                        e = e.right_;
                        continue;
                    }
                    result = treewalker(e.right_);
                    if (result)
                        break;
                }
                e = e.left_;
            } while (e);

            return result;
        }

        int result;
        foreach (e;table_)
        {
            if (e !is null)
            {
                result = treewalker(e);
                if (result)
                    break;
            }
        }
        return result;
	}


	public K[] keys()
	{
		K[] keyBlock = new K[](this.nodes_);
		size_t ix = 0;

        void keys_x(NodePtr e)
        {
            do
            {
                keyBlock[ix++] = e.key_;
                if (e.left_)
                {   if (!e.right_)
                    {   e = e.left_;
                        continue;
                    }
                    keys_x(e.left_);
                }
                e = e.right_;
            }
            while (e !is null);
        }
        if(keyBlock.length > 0)
        {

            foreach(e ; table_)
            {
                if (e !is null)
                {
                    keys_x(e);
                }
            }
        }

		return keyBlock;
	}

	public V[] values()
	{
		V[] valueBlock = new V[](this.nodes_);
		size_t ix = 0;

        void values_x(NodePtr e)
        {
            do
            {
                valueBlock[ix++] = e.value_;
                if (e.left_)
                {   if (!e.right_)
                    {   e = e.left_;
                        continue;
                    }
                    values_x(e.left_);
                }
                e = e.right_;
            }
            while (e !is null);
        }
        if(valueBlock.length > 0)
        {
            foreach(e ; table_)
            {
                if (e !is null)
                {
                    values_x(e);
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
	aaht!(int, int) aa;
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
