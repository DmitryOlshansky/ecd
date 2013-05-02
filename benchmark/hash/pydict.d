module hash.pydict;

import core.memory;
import core.exception;
import std.traits;
import core.stdc.string : memset;

import hash.util;
import std.c.stdlib;
import std.stdio;

/**
 This file includes a closed hash map implementation based on Pythons dictionary implementation.
 License of this file: Public Domain.

 Author: Moritz Warning
 Based on PyDict Ported to D by

 Compiler command used for testing with gdc:
 gdc main.d -o main -fversion=Posix -fversion=Tango -I/home/user/local/include/d/4.1.2 -L/home/user/local/lib -lgtango -ffast-math -O3 -frelease -finline-functions -msse -mfpmath=sse

 The size_t hash value gives access to a sequence of entries in a single big table.
 For keys with hash collision, the peturbation step tries each of the sequence, starting from the hashed value, and wraps around.
 The sequence appears to eventually try all the entry values, but table size checks ensure their are always some free.
 Initially all the key values are zero or null, the special unused_hash , so this cannot be a key in the table. If a key generates this hash,
 such as passing null for a pointer or array, or a zero integer.  then the key, value is stored apart from the Entry table.

 Similarly dummy_hash (1) is a special entry indicating the Entry was used and then later was removed. When doing an insertion or lookup, it indicates
 that there might be another key to check with same hash following in the peturb sequence. So the look_dict checks further for a match.
 Therefore an entry with a real hash 1 must be stored independently of the main table.


 The original PyDict defined AA.length = used + is_dummy + is_unused, where used is the number
 of active entries in the table. I have turned this around, to the mathematically
 equivalent used_entries = AA.length - is_dummy - is_unused.

---


---
 Calculated hash values are othersize tweaked if they correspond to the special hash values.

 When the struct for K,V has no pointers, it will not be scanned by the GC.
*/



class AAKeyError : Error {
	this(string msg)
	{
		super(msg);
	}
}

private enum : size_t {
	unused_hash = 0,
	dummy_hash = 1
}
/// template wrappers for keys. Each of these holds a piece of data and has
/// a hash property (which may be the same as the data for < integer.sizeof)
private struct ArrayWrapper(T)
{
    T data;
    size_t hash;

    void ctor()
    {
        static assert(isDynamicArray!(T) || isStaticArray!(T));

        if(data.length == 0)
        {
            if(cast(size_t) cast(void*) data.ptr == unused_hash)
            {
                hash = unused_hash;
                return;
            }
            else if(cast(size_t) cast(void*) data.ptr == dummy_hash)
            {
                hash = dummy_hash;
                return;
            }
        }

        //hash function
        ubyte[] a = cast(ubyte[]) data;
        int len = a.length;
        ubyte* p = cast(ubyte *) a.ptr;
        hash = *p << 7;
        while (--len >= 0)
        {
            hash = (1000003 * hash) ^ *p++;
        }
        hash ^= a.length;

        //avoid special hashes
        if(hash < 2) hash += 2;
    }

    void markDummy()
    {
        this.hash = dummy_hash;
        this.data = null;
    }

    alias typeof(this) TT;
    static bool cmp1(TT a, TT b) { return (a.hash == b.hash && a.data == b.data); }
    static bool cmp2(TT a, TT b) { return false; }
    static bool cmp3(TT a, TT b) { return (a.hash == b.hash && a.data == b.data); }
}
/*
private bool isSpecialKey(KW)(ref const KW key)
{
    return (key.hash < 2);
}

private bool isDummyKey(KW)(ref const KW key)
{
    return (key.hash == dummy_hash);
}

private bool isUnusedKey(KW)(ref const KW key)
{
    return (key.hash == unused_hash);
}
*/
private struct BigPODWrapper(T)
{
    T data;
    size_t hash;

    void ctor()
    {
        static assert(T.sizeof >= size_t.sizeof);

        hash = typeid(T).getHash(&data);

        //will work for ulong with additional hash
        //hash = *cast(size_t*) &data + (cast(size_t*) &data)[1];

        //avoid special hashes
        if(key.hash < 2) hash += 2;
    }

    void markDummy()
    {
        this.hash = cast(T) dummy_hash;
    }

    alias typeof(this) TT;
    static bool cmp1(TT a, TT b) { return a.data == b.data; }
    static bool cmp2(TT a, TT b) { return false; }
    static bool cmp3(TT a, TT b) { return a.data == b.data; }
}

//byte, uint etc. on 32bit
private struct SmallPODWrapper(T)
{
    T data;
    alias data hash;

    void ctor()
    {
        static assert(T.sizeof <= size_t.sizeof);// && !isPointerType!(T));
    }

    void markDummy()
    {
        this.data = cast(T) dummy_hash;
    }

    alias typeof(this) TT;
    static bool cmp1(TT a, TT b) { return a.data == b.data; }
    static bool cmp2(TT a, TT b) { return false; }
    static bool cmp3(TT a, TT b) { return a.data == b.data; }
}

private struct PointerWrapper(T)
{
    T data;

    void ctor()
    {
        static assert(isReferenceType!(T));
    }

    void markDummy()
    {
        void* tmp = cast(void*) dummy_hash;
        this.data = cast(T) tmp;
    }

    size_t hash() { return cast(size_t) cast(void*) data; }

    alias typeof(this) TT;
    static bool cmp1(TT a, TT b) { return a.data is b.data; }
    static bool cmp2(TT a, TT b)
    {
        static if(is(a.opEquals))
        {
            return a.opEquals(b);
        }
        else
        {
            return false;
        }
    }
    static bool cmp3(TT a, TT b)
    {
        static if(is(a.opEquals))
        {
            return a.data is b.data || a.opEquals(b);
        }
        else
        {
            return a.data is b.data;
        }
    }
}
private struct ClassWrapper(T)
{
    T data;
    size_t hash;

    void ctor()
    {
        this.hash = data.toHash();
    }

    void markDummy()
    {
        this.hash = 1;
        this.data = T.init;
    }

    size_t getHash() { return hash; }

    alias typeof(this) TT;
    //static bool cmp1(TT a, TT b) { return (a is b) || (a.data.opEquals(b.data)); }
    static bool cmp1(TT a, TT b) { return typeid(T).equals(&a.data, &b.data); }
    static bool cmp2(TT a, TT b) { return false; }
    static bool cmp3(TT a, TT b) { return typeid(T).equals(&a.data, &b.data); }
    //static bool cmp3(TT a, TT b) { return (a is b) || (a.data.opEquals(b.data)); }
}

private struct GenericWrapper(T)
{
    T data;
    size_t hash;

    void ctor()
    {
        this.hash = typeid(T).getHash(&data);
    }

    void markDummy()
    {
        this.hash = 1;
        this.data = T.init;
    }

    size_t getHash() { return hash; }

    alias typeof(this) TT;
    static bool cmp1(TT a, TT b) { return cast(bool) typeid(T).equals(&a.data, &b.data); }
    static bool cmp2(TT a, TT b) { return false; }
    static bool cmp3(TT a, TT b) { return cast(bool) typeid(T).equals(&a.data, &b.data); }
}

template SelectKeyWrapper(K)
{
    static if (isDynamicArray!(K) || isStaticArray!(K))
    {
        alias ArrayWrapper!(K) type;
    }
    else static if (isReferenceType!(K))
    {
        alias PointerWrapper!(K) type;
    }
    else static if (is(K == class))
    {
        alias ClassWrapper!(K)  type;
    }
    else static if (K.sizeof <= size_t.sizeof)
    {
        alias SmallPODWrapper!(K) type;
    }
    else static if (K.sizeof > size_t.sizeof)
    {
        alias BigPODWrapper!(K) type;
    }
    else // does not get here?
    {
        //uses TypeInfo
        alias GenericWrapper!(K) type;
    }
}

struct aapy(K,V)
{
    alias PyDict!(K,V) AAClassImpl;

    alias SelectKeyWrapper!(K).type KW;

    AAClassImpl  imp_;

	V* opIn_r(K k)
	{
		//wrap
		if (imp_ is null)
            return null;

        return imp_.opIn_r(k);
	}


    V opIndex(K k)
    {
        if (imp_ !is null)
        {
            V*  result = imp_.opIn_r(k);
            if (result !is null)
                return *result;
        }
        throw new AAKeyError("no key for opIndex");
    }
	void opIndexAssign(V value, K k)
	{
		//wrap key
		auto key = KW(k);
		key.ctor();

		if (imp_ is null)
            imp_ = new AAClassImpl();

        imp_.putKeyWrapValue(value, key);
	}

    void clear()
    {
        if (imp_ !is null)
            imp_.clear();

    }
	bool remove(K k)
	{
		auto key = KW(k);
		key.ctor();
        return imp_.removeKeyWrap(key);
	}
    @property void rehash()
    {
	    if (imp_ !is null)
            imp_.rehash;
    }
    @property aapy allocate()
    {
        aapy result;
        result.imp_ = new AAClassImpl();
        return result;
    }
    @property aapy init()
    {
        aapy result;
        return result;
    }

    @property size_t capacity()
    {
        if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.capacity;
    }
    @property void loadRatio(double ratio)
    {
        //dummy
    }
    @property void capacity(size_t cap)
    {
	    if (imp_ is null)
            imp_ = new AAClassImpl();
        imp_.capacity(cap);
    }

    @property uint[] list_stats()
    {
        if (imp_ is null)
            return null;
        //'return imp_.used;
        return imp_.list_stats;
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

	@property size_t length()
	{
	    if (imp_ is null)
            return 0;
        return imp_.length_;
	}

    @property aapy dup()
    {
        aapy copy;

        if (imp_ !is null)
        {
            copy.imp_ = new AAClassImpl(imp_);
        }
        return copy;
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
class PyDict(K, V)
{
private:
	//key wrapper type
	alias SelectKeyWrapper!(K).type KW;
    alias typeof(this)   SelfType;

	//need to be a power of two
	enum : size_t {MINSIZE = 8, PERTURB_SHIFT = 5}
	enum : real {PYDICT_CAPACITY = 0.6}

	struct Entry
	{
		KW key;
		V value;

	}

	//active + dummy entries
	size_t fill = 0;

	//Number of active nodes in the table plus is_unused plus is_dummy
	size_t length_ = 0;

	/*
	* The table contains mask + 1 slots, and that's a power of 2.
	* We store the mask instead of the size because the mask
	* is more frequently needed.
	*/

	Entry[] table;

	uint   capacity_; // point at which a rehash will be triggered
	/*
	* Since this.table can't hold entries for both special keys,
	* they have to be stored and handled separately.
	*/
	bool is_unused = false;
	bool is_dummy = false;

	Entry  eUnusedHash = Entry.init;
	Entry  eDummyHash =  Entry.init;

	public this()
	{
	}

    public this(SelfType c)
    {
        foreach(k,v ; c)
            opIndexAssign(v,k);

    }

	/*
	* Any key that is not special is active.
	*/
	private static bool isActiveKey(KW key)
	{
		return (key.hash > 1);
	}

debug {
    @property public size_t footprint() { return GC.sizeOf(table.ptr);}
    @property public size_t nodecount() { return table.length; }
}
    @property public size_t length(){ 	return length_; }

    private final void putKeyWrapValue(ref V value, ref KW key)
    {

        if (key.hash < 2)
		{
			if (key.hash == 0)
			{
				is_unused = true;
				eUnusedHash.key = key;
				eUnusedHash.value = value;

				++this.length_;
				return;
			}
			else //must be dummy
			{
				assert(key.hash == 1);
				is_dummy = true;
				eDummyHash.key = key;
				eDummyHash.value = value;
				++this.length_;
				return;
			}
		}

		Entry* ep = lookdict(key);
		assert(ep);

		if (isActiveKey(ep.key))
		{
			ep.value = value;
		}
		else
		{
			if (ep.key.hash==0)
			{
				++this.fill;
			}
			else
			{
				assert(ep.key.hash == 1);
			}

			ep.key = key;
			ep.value = value;

			++this.length_;
			checkLoad();
		}
    }
    private static void freeTable(ref Entry[] ntable)
    {
        delete ntable;
        ntable = null;
    }
    private static Entry[] dupTable(in Entry[] copied)
    {

        return copied.dup;

    }
    private static Entry[] allocTable(size_t newsize)
    {
        return new Entry[newsize];
    }
	/*
	* Lookup an entry in the table.
	* This is the workhorse.
	*/
	private final Entry* lookdict(KW key)
	{
		assert(key.hash >= 2);

		size_t hash = key.hash;

		size_t perturb = void;
		Entry *freeslot = void;

		if (table is null)
		{
		    table = allocTable(getNextPrime(1));
		    capacity_ = cast(size_t) (table.length * PYDICT_CAPACITY);
		}


        immutable tab_size = table.length;

		Entry *ep0 = this.table.ptr;

		//size_t i = hash & mask;
		size_t i = hash % tab_size;

		Entry *ep = &ep0[i];

		/*
		* This first lookup will succeed in the very most cases.
		*/
		if (ep.key.hash == 0 || KW.cmp1(ep.key, key))
		{
			return ep;
		}

		if (ep.key.hash == 1)
		{
			freeslot = ep;
		}
		else
		{
			if (KW.cmp2(ep.key, key))
			{
				return ep;
			}

			freeslot = null;
		}

		/*
		* In the loop, key == dummy is by far (factor of 100s) the
		* least likely outcome, so test for that last.
		*/
		for (perturb = hash; ; perturb >>= PERTURB_SHIFT)
		{
			i = (i << 2) + i + perturb + 1;
			//ep = &ep0[i & mask];
            ep = &ep0[i % tab_size];
			if (ep.key.hash == 0)
			{
				return (freeslot is null) ? ep : freeslot;
			}

			if (KW.cmp3(ep.key, key))
			{
				return ep;
			}

			if (freeslot is null && ep.key.hash == 1)
			{
				freeslot = ep;
			}
		}
		assert(0);	//never reached
	}

	public final V* opIn_r(K k)
	{
		//wrap
		auto key = KW(k);
		key.ctor();

        auto hashval = key.hash;
		if (hashval < 2)
		{
			if (hashval == 0)
			{
				return is_unused ? &eUnusedHash.value : null;
			}
			else //must be dummy
			{
				assert(hashval == 1);
				return is_dummy ? &eDummyHash.value : null;
			}
			assert(0);
		}

		Entry* ep = lookdict(key);
		assert(ep);

		if (ep.key.hash > 1)
		{
			return &ep.value;
		}
		else
		{
			return null;
		}
	}
    public final V get(K k)
    {
		V* p = opIn_r(k);
		if (p !is null)
		{
			return *p;
		}
		return V.init;
    }
	 public final bool get(K k, ref V val )
	{
		V* p = opIn_r(k);
		if (p !is null)
		{
			val = *p;
			return true;
		}
		val = V.init;
		return false;
	}

	 public final V opIndex(K k)
	{
		V* val = opIn_r(k);
		if (val is null)
		{
			throw new AAKeyError("invalid key");
		}
		return *val;
	}


	 public void opIndexAssign(V value, K k)
	{
		//wrap key
		auto key = KW(k);
		key.ctor();

        putKeyWrapValue(value, key);
	}

    @property  public void rehash()
    {
        //make table smaller, table size > MINSIZE and load factor is < 1/8
        size_t usedEntries = this.length_ - is_unused - is_dummy;
        if (usedEntries > capacity_)
        {
            dictresize(capacity_ * 2);
        }
		if (table.length > MINSIZE && usedEntries < capacity_ / 4)
		{
			dictresize(usedEntries / (usedEntries > 50000 ? 4 : 2));
		}
		else {
            //rehash in place?
            dictresize(usedEntries);
		}
    }
	/*
	* Check load factor and allocate new table
	*/
	private void checkLoad()
	{
        size_t usedEntries = this.length_ - is_unused - is_dummy;
		if (usedEntries > capacity_)
		{
			dictresize(2 * capacity_);
		}
	}
    public @property uint[] list_stats()
    {
        uint[] result = new uint[2];

        result[0] = table.length;
        result[1] = table.length - length_;

        return result;
    }

    private static freeEntry(ref Entry ep)
    {
        ep.key = KW.init;
        ep.value = V.init;
    }
    private bool removeKeyWrap(ref KW key)
    {


 		if (key.hash < 2)
		{
			if (key.hash == 0)
			{
				is_unused = false;
                freeEntry(eUnusedHash);
			}
			else //must be dummy
			{
				is_dummy = false;
                freeEntry(eDummyHash);
			}
            --this.length_;
            return true;
		}

		Entry* ep = lookdict(key);

		bool result = ep.key.hash >= 2; // special keys are empty entries in table

        if (result)
        {
            freeEntry(*ep);
            ep.key.markDummy();
            --this.length_;
        }

		return result;
    }

    // wipe out
    private void clean()
    {
        fill = 0;
        length_= 0;
        is_unused = false;
        freeEntry(eUnusedHash);

        is_dummy = false;
        freeEntry(eDummyHash);
        freeTable(table);
    }

    public void clear()
    {
        clean();
    }


	public void remove(K k)
	{
		//wrap
		auto key = KW(k);
		key.ctor();
        removeKeyWrap(key);
	}

    // increase the table size as required to support this number of entries without a rehash
    // this can make table very big, first get load capacity, and then next prime size
    void  capacity(size_t cap)
    {
        size_t realsize = cast(size_t)(cap / PYDICT_CAPACITY);
        if (realsize == 0)
            realsize = 1;
        dictresize(realsize);
    }

    size_t capacity()
    {
        return capacity_;
    }
	private void dictresize(size_t minused)
	{
		// Find the smallest table size > minused and size == 2**n.
        size_t newsize = getNextPrime(minused);

		// Get space for a new table.
		Entry[] oldtable = this.table;
		assert(oldtable !is null);

		Entry[] newtable = allocTable(newsize);
        capacity_ = cast(size_t)(newtable.length * PYDICT_CAPACITY);

		assert(newtable !is null);
		assert(newtable.ptr != oldtable.ptr);

		this.table = newtable;

		//memset(newtable, 0, Entry.sizeof * newsize);

		this.length_ = is_unused + is_dummy;
		size_t i = this.fill;
		this.fill = 0;

		//copy the data over; filter out dummies
		for (Entry* ep = oldtable.ptr; i > 0; ep++)
		{
			if (isActiveKey(ep.key))
			{
				--i;
				insertdict_clean(ep.key, ep.value);
			}
			else if (ep.key.hash == 1)
			{
				--i;
			}
		}

		freeTable(oldtable);
	}

	/*
	* Insert an item which is known to be absent from the dict.
	* This routine also assumes that the dict contains no deleted entries.
	*/
	private void insertdict_clean(KW key, V value)
	{
		assert(key.hash >= 2);

		size_t hash = key.hash;
        immutable tab_size = table.length;

		Entry* ep0 = this.table.ptr;
        size_t perturb = hash;

		size_t i = hash %  tab_size;
		Entry* ep = &ep0[i];

		while(ep.key.hash > 0)
		{
			i = (i << 2) + i + perturb + 1;
			ep = &ep0[i % tab_size];
			perturb >>= PERTURB_SHIFT;
		}

		++this.fill;
		ep.key = key;
		ep.value = value;
		++this.length_;
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
		Entry* ep = this.table.ptr;
		int result = 0;

		if (is_unused)
		{
			auto key = cast(K_) eUnusedHash.key.data;
			auto value = cast(V_) eUnusedHash.value;
			result = dg(key, value);

			if(result != 0)
			{
				return result;
			}
		}

		if (is_dummy)
		{
			auto key = cast(K_) eDummyHash.key.data;
			auto value = cast(V_) eDummyHash.value;
			result = dg(key, value);

			if(result != 0)
			{
				return result;
			}
		}

        immutable size_t tab_size = table.length;
		for (size_t i = 0; i < tab_size; ++i)
		{
			if (ep[i].key.hash < 2)
			{
				continue;
			}

			auto key = cast(K_) ep[i].key.data;
			auto value = cast(V_) ep[i].value;
			result = dg(key, value);

			if (result != 0)
			{
				break;
			}
		}

		return result;
	}

	/*
	* Get number of active entries stored.
	*/
	public size_t size()
	{
		return length_;
	}

	public K[] keys()
	{
		K[] keys = new K[](this.length);
		Entry* ep = this.table.ptr;
		immutable tab_size = table.length;
		size_t n = 0;

		if (is_unused) keys[n++] = cast(K_) eUnusedHash.key.data;
		if (is_dummy) keys[n++] = cast(K_) eDummyHash.key.data;

		for (size_t i = 0; i < tab_size; ++i)
		{
			if (ep[i].key.hash < 2)
			{
				continue;
			}
			keys[n] = ep[i].key.data;
			++n;
		}

		return keys;
	}

	public V[] values()
	{
		V[] values = new V[](this.length);
		Entry* ep = this.table.ptr;
		immutable tab_size = table.length;
		size_t n = 0;

		if (is_unused) values[n++] = eUnusedHash.value;
		if (is_dummy) values[n++] = eDummyHash.value;

		for (size_t i = 0; i < tab_size; ++i)
		{
			if (ep[i].key.hash < 2)
			{
				continue;
			}
			values[n] = ep[i].value;
			++n;
		}

		return values;
	}
}

template isReferenceType(T) {
    enum isReferenceType = is(T : const(void*))  ||
                           //is( T == class )     ||
                           is( T == interface ) ||
                           is( T == delegate );
 }


void unit_test()
{
	//
	aapy!(int, int) aa;
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
