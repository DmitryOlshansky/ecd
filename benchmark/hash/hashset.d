
/**
 * Copyright: Copyright Digital Mars 2000 - 2010.
 * License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.

  This is like the hashtable implementation, but only stores a key.
  It stores entries directly in the table where possible.
  or otherwise link chains them.

  References to the keys should not be stored by code, since operations may move the
  data nodes.

  Remove is not supported during iteration with opApply, foreach, etc.

  Integer indexes are stored and directly used as the hash.

  authors: Michael Rynn
	


  
*/

module hash.hashset;

import core.memory;
import core.exception;
import std.traits;
import core.stdc.string : memset, memcpy;
import hash.util;
import std.stdio;
import std.variant;

//version = NodeHeap;
version = miss_stats;
version = prime_size;

version(NodeHeap)
{
    import tangy.util.container.Container;
    pragma(msg, "hashtablist uses NodeHeap");
}

private {
    extern (C) void* gc_malloc( size_t sz, uint ba = 0 );
    extern (C) void* gc_calloc( size_t sz, uint ba = 0 );
    extern (C) void  gc_free( void* p );
}





class AAKeyError : Error {
	this(string msg)
	{
		super(msg);
	}
}

/// The node structure for the hash table trees.

private immutable Variant sNullVariant;

private import std.c.stdlib;

private struct HtlEntry(K)
    {
        //alias HtlEntry*    EntryPtr;
        static if (K.sizeof > size_t.sizeof || is(K==class) || is(K==interface))
            hash_t hash_;

        HtlEntry*  next_;
        K      key_;

        void swap(HtlEntry* other)
        {
            static if (K.sizeof > size_t.sizeof || is(K==class) || is(K==interface))
            {
                hash_t temph = other.hash_;
                other.hash_ = hash_;
                hash_ = temph;
            }


            HtlEntry* temp = other.next_;
            other.next_ = next_;
            next_ = temp;

            K tempkey = other.key_;
            other.key_ = key_;
            key_ = tempkey;

        }
        void wipe()
        {
            next_ = null;
			static if(!is(K == Variant))
				key_ = K.init;
			else
				key_ = sNullVariant;

            static if (K.sizeof > size_t.sizeof || is(K==class) || is(K==interface))
                hash_ = 0;

        }

    }

struct HashSet(K)
{
    private alias HashSetImpl!(K)   ClassImpl;
    alias HtlEntry!(K)*    NodePtr;

    private ClassImpl  imp_;

	// opIn_r can make sense as a (temporary) pointer to the key 
	K* opIn_r(K k)
	{
		//wrap
		if (imp_ is null)
            return null;
        NodePtr np = imp_.getNode(k);
        if (np is null)
            return null;
        return &np.key_;
	}

	// treat as a virtual boolean array, no need for exception if missing
    bool opIndex(K k)
    {
        if (imp_ !is null)
        {
            NodePtr np = imp_.getNode(k);
            if (np !is null)
                return true;
        }
        return false; 
    }
	// if value is true, insert the key if not available, else delete it 
	void opIndexAssign(bool value, K k)
	{
	    if (value)
		{
			if (imp_ is null)
				imp_ = new ClassImpl();
		}
		else {
			if (imp_ !is null)
				imp_.getNode(k,NodeOp.node_destroy);
		}
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
    @property void rehash()
    {
	    if (imp_ !is null)
            imp_.rehash;
    }
    @property HashSet init()
    {
        HashSet result;
        return result;
    }
    @property HashSet allocate()
    {
        HashSet result;
        result.imp_ = new ClassImpl();
        return result;
    }
    @property
	K[] keys()
	{
	    if (imp_ is null)
            return null;
        return imp_.keys();
	}
    bool put(K k)
     {
		if (imp_ is null)
            imp_ = new ClassImpl();
		size_t preNodes = imp_.nodes_;
		NodePtr e = imp_.getNode(k, NodeOp.node_create);
		return (imp_.nodes_ > preNodes);
     }	
	/** return stored key if it exists, store it if it doesn't */
	K get(K key)
	{
		if (imp_ is null)
            imp_ = new ClassImpl();
		NodePtr e = imp_.getNode(key, NodeOp.node_create);
		return e.key_;
	}
	
	
    @property size_t capacity()
    {
        if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.capacity;
    }

	@property bool contains(K k)
	{
		return (imp_ is null) ? false : (imp_.getNode(k) !is null);
	}
	
    @property void capacity(size_t cap)
    {
	    if (imp_ is null)
            imp_ = new ClassImpl();
        imp_.capacity(cap);
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
            imp_ = new ClassImpl();
        imp_.loadRatio(ratio);
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


    @property HashSet dup()
    {
        HashSet copy;

        if (imp_ !is null)
        {
            copy.imp_ = new ClassImpl(imp_);
        }
        return copy;
    }
    bool remove(K key)
    {
		return (imp_ is null) ? false : imp_.remove(key);
    }
	
	public int opApply(int delegate(ref K key) dg)
	{
		return (imp_ !is null) ? imp_.opApply(dg) : 0;
	}
}

/// implementation as a class

enum NodeOp { node_find, node_create, node_destroy }

class HashSetImpl(K)
{
private:
    alias typeof(this)   SelfType;

private {


	alias HtlEntry!(K)  Node;
    alias Node*        NodePtr;
}


    enum : double { LOAD_FACTOR = 1.0}

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


    enum : size_t { null_chain = size_t.max }
    
	Node[]      table_;
    size_t      nodes_;
    size_t      capacity_;  // how many nodes before next rehash
    double     load_factor_;  // a ratio of capacity to indication of fullness
    //size_t      mask_;
    // TODO: replace this with TypeInfo_AssociativeArray when available in _aaGet()
    TypeInfo    keyti_;

    version(miss_stats)
    {
         size_t      max_misses_;
         size_t      rehash_ct_;
    }

	public this(size_t cap = 0)
	{
	    load_factor_ = LOAD_FACTOR;
		keyti_ = typeid(K);
		if (cap > 0)
		{
		    capacity(cap);
		}
	}

    public this(SelfType copyme)
    {
        load_factor_ = copyme.load_factor_;
        keyti_ = typeid(K);
        capacity(copyme.capacity);
        foreach(k ; copyme)
            this.getNode(k, NodeOp.node_create);
    }
    @property size_t capacity()
    {
        return capacity_;
    }

    /// Uses loadFactor to calculate
    /// cannot be used to reduce capacity
    @property void capacity(size_t cap)
    {
        size_t nlen = cast(size_t)(cap / load_factor_);

        nlen = getNextPrime(nlen);
        resizeTable(nlen);
    }

    /// Does not trigger a rehash or resize directly.
    @property void loadRatio(double ratio)
    {
        if (ratio < 0.5)
            ratio = 0.5;
        else if (ratio > 16.0)
            ratio = 16.0;
        load_factor_ = ratio;
    }

    @property
    public double loadRatio()
    {
        return load_factor_;
    }
	
    @property
    public final size_t length()
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

            foreach(ref e ; table_)
            {
                if(e.next_ !is null) // entry is occupied
                {
                    uint listct = 1;
                    NodePtr np = e.next_;

                    if (np != cast(NodePtr) null_chain)
                    {
                        while(np !is null)
                        {
                            listct++;
                            np = np.next_;
                        }

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


    /// return a node pointer. Null if not found, or cannot create
    /// if create was true, set it to false if the node was not created
    private static void freeTable(ref Node[] ntable)
    {

        delete ntable;
        ntable = null;

    }

    private static Node[] allocTable(size_t newsize)
    {
        return new Node[newsize];
    }
    /** Return a pointer to a existing node, or null, if create is false.
    Return a pointer to existing node, or create a new node with the key, if create is true.
    */
	
	
	private final NodePtr getNode(K key, NodeOp op = NodeOp.node_find)
	{
	    if (table_ is null)
	    {
	        if (op == NodeOp.node_create)
	        {
	            version(prime_size)
                    size_t nlen = getNextPrime(1);
                else {
                    size_t nlen = 128;
                    mask_ = 127;
	            }
	            table_ = new Node[nlen];
	            capacity_ = cast(size_t)(table_.length * load_factor_);     // capacity tolerance
	        }
            else
                return null;
	    }
		else if ((op == NodeOp.node_create) && (nodes_ >= capacity_))
		{
			capacity(nodes_ * 4);
		}

        immutable table_size = table_.length;

        static if (K.sizeof > size_t.sizeof || is(K==class) || is(K==interface))
        {
            hash_t key_hash = keyti_.getHash(&key);
        }
        else {
            hash_t key_hash = overhash(key);
        }
        size_t my_chair = key_hash;


        version(prime_size)
            size_t nix = (my_chair % table_size);
        else
            size_t nix = (my_chair & mask_);

        NodePtr e = &table_[nix];       // last node tested
        NodePtr np = e.next_;

        version(miss_stats)
            size_t misses = 0;

        bool match_e()
        {
            static if (K.sizeof > size_t.sizeof || is(K==class) || is(K==interface))
            {
                if ((key_hash == e.hash_) && (keyti_.compare(&key, &e.key_)==0))
                //if ((key_hash == e.hash_) && keyti_.equals(&key, &e.key_))
                {
                    return true;
                }
            }
            else {
                if (key == e.key_)
                {
                    return true;
                }
            }
            return false;
        }

        NodePtr extra_e()
        {
            version(NodeHeap)
                NodePtr e2 = heap.allocate();
            else
                NodePtr e2 = cast(Node*)gc_calloc(Node.sizeof);
            e2.key_ = key;
            static if (K.sizeof > size_t.sizeof || is(K==class)|| is(K==interface))
                e2.hash_ = key_hash;
            e2.next_ = null;
            return e2;
        }

        if (np is null)
        {
            // prime table is empty
            if (op != NodeOp.node_create)
                return null;  // cannot find or destroy
            e.key_ = key;
            static if (K.sizeof > size_t.sizeof || is(K==class)|| is(K==interface))
                e.hash_ = key_hash;
            e.next_ = cast(NodePtr) null_chain;
            nodes_++;
            return e;
        }
        else if (np == cast(NodePtr) null_chain)
        {
            // only prime entry to consider
            bool matched = match_e();
            final switch(op)
            {
            case NodeOp.node_find:
                return matched ? e : null;
            case NodeOp.node_create:
                if (matched)
                    return e;
                else {
                    NodePtr e2 = extra_e();
                    e.next_ = e2;
                    nodes_++;
                    return e2;
                }
            case NodeOp.node_destroy:
                if (matched)
                {
					nodes_--;
                    return e;
                }
                else
                    return null;
            }
        }
        else { // chained search ending in null, at least one node in chain
            NodePtr *last_link = null; // track this in case of destroy
            size_t link_count = 0;
            for(; e !is null ;link_count++)
            {
                 np = e.next_;
                 if (match_e())
                 {
                    if (op != NodeOp.node_destroy)
                        return e;

                    if (link_count == 0) //
                    {
                        assert(np !is null);

                        // swap with node in prime table, to return a disposable node
                        e.swap(np);

                        if (e.next_ is null)
                            e.next_ = cast(NodePtr) null_chain;
                        e = np;
                    }
                    else if (link_count == 1)
                    {
                         if (np is null)
                         {
                            *last_link = cast(NodePtr) null_chain;
                         }
                         else {
                            *last_link = np;
                         }
                    }
                    else {
                        *last_link = np;
                    }
                    nodes_--;
                    return e;

                 }
                 last_link = cast(NodePtr*)&e.next_;
                 e = np;
            }
            // no matches, last_link
            if (op !=  NodeOp.node_create)
                return null;
            NodePtr e3 = extra_e();
            *last_link = e3;
            nodes_++;
            return e3;

        }
        assert(0);

	}

	/** Set existed to be true if the key did not exist before
		Return the value in the node.
	*/
	public final K set(K k, ref bool existed)
	{
		uint preCount = nodes_;
		NodePtr e = getNode(k, NodeOp.node_create);
		existed = (nodes_ > preCount);
		return e.key_;
	}
	public final bool put(K k)
	{
		auto preCount = nodes_;
		NodePtr e = getNode(k, NodeOp.node_create);
		return (nodes_ > preCount);
	}	
	public final K* opIn_r(K k)
	{
        NodePtr e = getNode(k);
        if (e is null)
            return null;
        return &e.key_;
	}

    public final K get(K k)
    {
		NodePtr e = getNode(k, NodeOp.node_create);
		return e.key_;
    }
	
	public final bool contains(K k)
	{
        return (getNode(k) !is null);
	}
	
	
	/+ 
    public final bool get(K k, ref V val )
	{
		NodePtr  e = getNode(k);

		if (e is null)
        {
			static if (!is(V==Variant))
				val = V.init;
            return false;
        }

        val = e.value_;
        return true;
	}
	+/

    public final bool opIndex(K k)
	{
		return getNode(k) != null;
	}
  /+
     private void assign(ref V value, K k)
     {

		NodePtr e = getNode(k, NodeOp.node_create);
        e.value_ = value;

        if (nodes_ > capacity_)
        {
            rehash();
        }
     }
	+/
	
	 public void opIndexAssign(bool value, K k)
	 {
		 getNode(k, value ? NodeOp.node_create : NodeOp.node_destroy);
	 }

	 // test if this is directly in the table, or a link
     private void trash(NodePtr e)
     {

         if ( (e < table_.ptr) || e >= (table_.ptr + table_.length))
         {
            // chained memory block
            version(NodeHeap)
            {
                heap.collect(e);
            }
         }
         else {
            // prime table block,
            e.wipe();
         }
     }

	 // return if the key existed before the call
     public bool remove(K k)
     {
         NodePtr e = getNode(k, NodeOp.node_destroy);
         if (e is null)
            return false;
         trash(e);  
         return true;
     }


    private void resizeTable(size_t nlen)
    {
        // Unfortunately, resize is no longer simple.
        // Some chained nodes will turn into prime,
        // Some prime nodes might be chained
        // rather than some more complex code, re-use getNode
        Node[] oldtable = table_;
        table_ = new Node[nlen];

        size_t oldnodes = nodes_;
        nodes_ = 0;
        capacity_ = cast(size_t)(table_.length * load_factor_);


        if (oldnodes)
		{
			uint checkct = 0;
			foreach (ref te; oldtable)
			{
				NodePtr np = te.next_;
				if (np !is null)
				{
					NodePtr e = getNode(te.key_,NodeOp.node_create);
					checkct++;
					if (np != cast(NodePtr) null_chain)
					{
						while (np !is null)
						{
							NodePtr temp = np;
							e = getNode(temp.key_,NodeOp.node_create);
							checkct++;
							np = np.next_;
							version(NodeHeap)
								heap.collect(temp);
						}

					}
				}
			}
		}
        freeTable(oldtable);
        assert(oldnodes == nodes_);
   }


    private void rehash()
    {
		capacity( cast(size_t) (nodes_ * load_factor_));
	}


    private void deleteAllNodes()
    {

        foreach(ref te ; table_)
        {
            if (te.next_ !is null)
            {
                NodePtr np = te.next_;

                te.next_ = null;
                if (np != cast(NodePtr) null_chain)
                {

                    do {
                        NodePtr e = np;
                        np = e.next_;
                        version(NodeHeap)
                            heap.collect(e);
                        else
                            gc_free(e);
                    }
                    while (np !is null);
                }
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

	public int opApply(int delegate(ref K_ key) dg)
	{

        int result;
        foreach (ref te;table_)
        {
            NodePtr np =  te.next_;

            if (np !is null)
            {

                result = dg(te.key_);
                if (result || nodes_ == 0)
                    break;

                if (np != cast(NodePtr) null_chain)
                {
                    do {
                        NodePtr e = np;
                        np = e.next_;
                        result = dg(e.key_);
                        if (result || nodes_ == 0)
                            break;
                    }
                    while (np !is null);
                }
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

            foreach(ref te ; table_)
            {
                NodePtr np = cast(NodePtr)te.next_;
                if (np !is null)
                {
                    keyBlock[ix++] = te.key_;
                    if (np != cast(NodePtr) null_chain)
                    {
                        do {
                            keyBlock[ix++] = np.key_;
                            np = np.next_;
                        }
                        while (np !is null);
                    }
                }
            }
        }
		return keyBlock;
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
	HashSet!(int) aa;
	bool  test;
	int   *ptest;
	// test unused key assign
	aa[0] = true;

	// test dummy key assign
	aa[1] = true;


	assert(aa.get(0));
	assert(aa.get(1));

	// test missing key

	string msg = "test missing key";

	assert (!aa.get(100),msg);

	try {
		test = aa[100];
		assert(0,"no missing key exception");
	}
	catch(AAKeyError e)
	{
		// ok
	}

	aa[100] = true;
	ptest = 100 in aa;
	assert(ptest !is null);

    auto keys = aa.keys;
	assert(keys[0..3] == [0,1,100]);

    auto bb = aa.dup;

    keys = bb.keys;
	assert(keys[0..3] == [0,1,100]);

	bb.clear;

	assert(aa.length == 3);
}
