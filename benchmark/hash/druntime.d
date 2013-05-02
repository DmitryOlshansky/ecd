/** Rather than being an actual template implementation,
 this provides access to extra functionality to the druntime A
 for tweaking via functions not directly utilised by the dmd compiler,
 but snuck in to a custom version of rt.aaA.

 Because the rt.aaA is written as a hidden implementation accessible only by C functions called from
 the Dmd compiler, the current interface is constrained towards this interface style.

 The idea is to work with the existing functions and the dmd compiler
 , but also to add some similar generic C in rt.aaA functions to
 give privileged access to extra functionality via templates here.

 This requires a special version of rt.aaA which uses extra functions and fields.


License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
Authors:   Michael Rynn

*/
module hash.druntime;


import hash.util;
import std.variant;

version=TEST_DRAA;

version(TEST_DRAA)
{
import std.stdio;
import std.conv;
}


private {

    /** This is the AA druntime C function interface.
        Original plus a few extras.

        Keeping with the convention of  anonymous access through a void*.


    */
    extern (D) typedef int delegate(void *) dg_t;
    extern (D) typedef int delegate(void *, void *) dg2_t;

    extern(C):
// Old interfaces
        /// return the nodes
        size_t _aaLen(void*);
        int _aaEqual(TypeInfo_AssociativeArray, void*, void*);
        bool _aaDelNode(void*,...);
        bool _aaDelGetValue(void*,...);

        /// return a pointer or null
        void* _aaGetNodeValue(void*, ...);

        /// do a serious wipe of the nodes and array
        void _aaClear(void*);

        void _aaDebug(void*, bool value);

        /// get the current capacity, or threshold for rehash
        size_t _aaGetCapacity(void*);



        /// Set a new capacity , and will probably trigger a rehash.
        void _aaSetCapacity(void*, size_t cap);

        /** Set the load ratio of node count to length of hash table.
          This value is used by capacity. A range of (0.5 - 8.0)
          Lower values use more memory to get better hash performance.
          Values of 4.0 and greater use up most of the hash buckets, but slow down
          with longer chains.
       */
        void _aaSetLoadRatio(void*, double ratio);

        /** insert or replace a new value.
         Caller must know initialisation has occurred.
         Returns a to the value location
         */

        double _aaGetLoadRatio(void*);

        void* _aaPutInsert(void*, ...);

        int _aaApplyOne(void*, dg_t dg);

        int _aaApplyTwo(void*, dg2_t dg);

        int _aaApplyKeys(void*, dg_t dg);

        alias long ArrayRet_t; // something to hold array ptr and length

        bool _aaContains(void*, ...);
        ArrayRet_t _aaGetKeys(void*);

        ArrayRet_t _aaGetValues(void*, TypeInfo valti);

        size_t _aaGetKeyValues(void*, ArrayRet_t*, ArrayRet_t* ,  TypeInfo valti);

        ArrayRet_t _aaStats(void*);

        /** Check if the key is small enough and not a special reference type.
            The key values can be stored directly in the hash field.
            This can be vetoed by _aaInitAA.
        */
        bool _aaDirectHashType(TypeInfo keyti);


        void _aaResize(void*);

        /// first arg is cast(void*) &aa

         /** Make a new AA with all the data of the old.
           Make aliases to old reference hold zero data.
        */

        void* _aaAssumeUnique(void*);

        void* _aaInitHashSet(void*,  TypeInfo keyti, bool hashSmall, uint preAllocate);

        void* _aaInitAA(void*, TypeInfo_AssociativeArray, bool hashSmall, uint preAllocate);

        /// take settings from another array
        void* _aaInitCopySetup(void**, void* );
}


/** A hash set, equivalent of (virtual bool)[keyType].
    This uses the builtin AA code with a value size of zero.
    The opIndex and OpIndexAssign treated as a boolean array.
*/

struct HashSet(K)
{
    private void* aa_;

    void setup(bool hashSmall = false, uint preAllocate = 0)
    {
        _aaInitHashSet(&aa_, typeid(K),  hashSmall, preAllocate);
    }
    bool isSetup()
    {
        return ( aa_ !is null);
    }

    bool contains(K key)
    {
        return _aaContains( aa_, key);
    }

    /// Insert key if not already present.  Return if it already existed.
    bool put(K key)
    {
        if (aa_ is null)
            setup();
        size_t preNodes = _aaLen(aa_);
        _aaPutInsert(aa_, key);
        return (preNodes < _aaLen(aa_));
    }
	/** This is a special get that puts in the value if it does not exist already.
		For only using one copy of a key, always return the original.
	**/
	K get(K key)
	{
	    if (aa_ is null)
            setup();	
		K* kret = cast(K*) _aaPutInsert(aa_, key);
		return *kret;
	}
    /// Remove key if it exists. Return if already existed.
    bool remove(K key)
    {
        if (aa_ !is null)
            return _aaDelNode(aa_, key);
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
        ArrayRet_t ka = _aaGetKeys(aa_);
        return *(cast(K[]*) &ka);
    }

    public int opApply(int delegate(ref K key) dg)
    {
        if (aa_ !is null)
        {
            return _aaApplyKeys(aa_, cast(dg_t) dg);
        }
        return 0;
    }

    /// Return if the value exists or not.
    bool opIndex(K key)
    {
        if (aa_ !is null)
        {
            return _aaContains( aa_, key);
        }
        return false;
    }

    /// assign true or false.  false == remove
    void opIndexAssign(bool value, K key)
    {
        if (value)
        {
            if (aa_ is null)
                setup();
            _aaPutInsert( aa_, key);
        }
        else {
            if (aa_ !is null)
                return _aaDelNode(aa_, key);
        }
    }

   /**Return a new hashset instance holding all data that originally belonged to the old.
    All old aliased references will point to the old, now empty AA.
    */
    @property static HashSet assumeUnique(ref HashSet other)
    {
        HashSet copy;

        copy.aa_ = _aaAssumeUnique( &other);
        return copy;
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
        _aaSetCapacity(aa_, cap);
    }
    /** Return threshold number of entries for automatic rehash after insertion.
    */
    @property size_t capacity()
    {
       if (aa_ is null)
         return 0;
       return _aaGetCapacity(cast(void*) aa_);
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
        _aaSetLoadRatio(aa_, ratio);
    }

    /**
        Return the current loadRatio
    */
    @property double loadRatio()
    {
        if (aa_ is null)
            return 0.0;
        else
            return _aaGetLoadRatio(aa_);
    }
    /**
        Return the number of entries
    */
    @property final size_t length()
    {
         return _aaLen(aa_);
    }
    /**
        Return <hash table length>, <empty buckets>,  <buckets of length 1>, [<buckets of length #>]
        Result will be of length 2 or more.
    */
    @property uint[] list_stats()
    {
        _aaDebug(aa_, true);
        ArrayRet_t result = _aaStats(aa_);
        return *(cast(uint[]*)&result);
        _aaDebug(aa_, false);
    }

    /** Return a new managed duplicate of all the entries */

    @property HashSet dup()
    {
        HashSet copy;
        if (aa_ is null)
            return copy;

         _aaInitCopySetup(&copy.aa_, aa_);

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
            _aaClear(aa_);
        }
    }

    /**
        Optimise table size according to current number of nodes and loadRatio.
    */
    @property void rehash()
    {
       if (aa_ !is null)
       {
           _aaResize(aa_);
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
            _aaPutInsert(aa_, key);
        }
    }
}


    /**
    This uses new C interface functions found in new version of aaA.d
    */

struct HashTable(V : V[K], K) {
    private alias V[K] vka;
    private void* aa_;

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
        //version(TEST_DRAA) writefln("setup DRHashMap hashSmall = %s alloc = %s",hashSmall, preAllocate);
        _aaInitAA(&aa_, cast(TypeInfo_AssociativeArray)typeid(vka) , hashSmall, preAllocate);
    }

    /// Return if the implementation is already initialised.
    bool isSetup()
    {
        return ( aa_ !is null);
    }

    /// support "in" operator
    V* opIn_r(K pkey)
    {
       return cast(V*) _aaGetNodeValue( aa_, pkey);
    }

    /// Return the value or throw exception
    V opIndex(K key)
    {
        if (aa_ !is null)
        {
            V* v = cast(V*) _aaGetNodeValue(aa_, key);
            if (v !is null)
            {
                return *v;
            }
        }
        throw new AAKeyError("no key for opIndex");
    }

    /// Insert or replace. Will call setup for uninitialised AA.
    void opIndexAssign(V value, K key)
    {
        if (aa_ is null)
            setup();
        V* v = cast(V*) _aaPutInsert( aa_, key);
        *v = value;
    }


    /// Insert or replace. Return true if insert occurred.
    bool putInsert(K key, ref V value)
    {
        if (aa_ is null)
            setup();
        size_t before_nodes = _aaLen(cast(void*) aa_);
        V* v = cast(V*) _aaPutInsert((cast(void*) aa_), key);
        *v = value;
        return (_aaLen(cast(void*) aa_) > before_nodes);
    }

    /// Insert or replace.
    void put(K key, ref V value)
    {
        if (aa_ is null)
            setup();

        V* v = cast(V*) _aaPutInsert((cast(void*) aa_), key);
        *v = value;
    }

    /// Get the value or throw exception
    V get(K key)
    {
        if (aa_ !is null)
        {
            V* v = cast(V*) _aaGetNodeValue(cast(void*) aa_, key);
            if (v !is null)
            {
                return *v;
            }
        }
        throw new AAKeyError("AA get failed");
    }
    /// Return if the key exists.
    bool contains(K key)
    {
        if (aa_ is null)
            return false;
        return _aaContains( aa_, key);
    }
    /// Get the value if it exists, false if it does not.
    bool get(K key, ref V val)
    {
        if (aa_ !is null)
        {
            V* v = cast(V*) _aaGetNodeValue( aa_, key);
            if (v !is null)
            {
                val = *v;
                return true;
            }
        }
        static if(!is(V == Variant))
            val = V.init;
        return false;
    }
    /**Return a new AA instance holding all data that originally belonged to the old.
    All old aliased references will point to the old, now empty AA.
    */
    @property static HashTable assumeUnique(ref HashTable other)
    {
        HashTable copy;

        copy.aa_ = _aaAssumeUnique( &other);
        return copy;
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

        _aaSetCapacity(aa_, cap);
    }
    /** Return threshold number of entries for automatic rehash after insertion.
    */
    @property size_t capacity()
    {
       if (aa_ is null)
         return 0;
       return _aaGetCapacity(cast(void*) aa_);
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
        _aaSetLoadRatio(aa_, ratio);
    }

    /**
        Return the current loadRatio
    */
    @property double loadRatio()
    {
        if (aa_ is null)
            return 0.0;
        else
            return _aaGetLoadRatio(aa_);
    }
    /**
        Return the number of entries
    */
    @property final size_t length()
    {
         return _aaLen(aa_);
    }

    /**
        Return the unmanaged AA object type
    */
    @property vka AA()
    {
        return *(cast(vka*) &aa_);
    }

    /**
        Wrap and manage a type-checked AA object
    */
    @property void AA(vka daa)
    {
        aa_ = cast(void*) daa;
    }

    /** Return a new managed duplicate of all the entries */

    @property HashTable dup()
    {
        HashTable copy;
        if (aa_ is null)
            return copy;

         _aaInitCopySetup(&copy.aa_, aa_);

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
            _aaClear(aa_);
        }
    }

    /**
        Optimise table size according to current number of nodes and loadRatio.
    */
    @property void rehash()
    {
       if (aa_ !is null)
       {
           _aaResize(aa_);
       }
    }
    /// Test if both arrays are empty, the same object, or both have same data
    bool equals(ref HashTable other)
    {
       if (!isSetup())
            return (!other.isSetup());
       else if (!other.isSetup())
            return false;

       return _aaEqual(typeid(vka), aa_, other.aa_) ? true : false;
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
        if (aa_ !is null)
            return _aaDelNode(aa_, key);
        return false;
    }

    /** Return the value and remove the key at the same time.
        Return false if no key found.
    */

    bool remove(K key, ref V value)
    {
        if (aa_ !is null)
        {
            if (_aaDelGetValue(aa_, key, &value))
            {
                return true;
            }
        }
        static if(!is(V == Variant))
            value = V.init;
        return false;
    }
    /**
        foreach_key( no such thing )
        // key cannot be a reference because it cannot be changed.

    */
    public int eachKey(int delegate(ref K key) dg)
    {
        if (aa_ !is null)
        {
            return _aaApplyKeys(aa_, cast(dg_t) dg);
        }
        return 0;
    }
    /**
        foreach(value)
    */
    public int opApply(int delegate(ref V value) dg)
    {
        if (aa_ !is null)
        {
            return _aaApplyOne(aa_, cast(dg_t) dg);
        }
        return 0;
    }
    /**
        foreach(key, value)
    */
    public int opApply(int delegate(ref K key, ref V value) dg)
    {
        if (aa_ !is null)
        {
            return _aaApplyTwo(cast(void*) aa_, cast(dg2_t) dg);
        }
        return 0;
    }

    /**
        Return all keys.
    */
    @property
    K[] keys()
    {
        if (aa_ is null)
            return null;
        ArrayRet_t ka = _aaGetKeys(aa_);
        return *(cast(K[]*) &ka);
    }
    /**
        Return all values.
    */
    @property
    V[] values()
    {
        if (aa_ is null)
            return null;
        ArrayRet_t va = _aaGetValues(aa_, typeid(V));
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
        return _aaGetKeyValues(aa_, cast(ArrayRet_t*) &ka, cast(ArrayRet_t*) &va, typeid(V));
    }

    /**
        Return <hash table length>, <empty buckets>,  <buckets of length 1>, [<buckets of length #>]
        Result will be of length 2 or more.
    */
    @property uint[] list_stats()
    {
        _aaDebug(aa_, true);
        ArrayRet_t result = _aaStats(aa_);
        return *(cast(uint[]*)&result);
        _aaDebug(aa_, false);
    }
}


version(TEST_DRAA)
void unittest_builtin()
{
    bool testsOK = true;
    void test_failed(string msg)
    {
        writeln(msg);
        testsOK = false;
    }




    HashTable!(char[char])   aaChar;

    for(char x = 'a'; x <= 'z'; x++)
        aaChar[x] = cast(char)(x + ('Z' - 'z'));

    for(char x = 'a'; x <= 'z'; x++)
        if (aaChar[x] != cast(char)(x + ('Z' - 'z')))
        {
            test_failed("char[char] failed");
            break;
        }

    auto meToo = aaChar.dup;

    for(char x = 'a'; x <= 'z'; x++)
        if (meToo[x] != cast(char)(x + ('Z' - 'z')))
        {
            test_failed("char[char] dup failed");
            break;
        }

    HashTable!(short[short]) aaShort;

    writeln("Short test");
    for(short ix1 = 100; ix1 <= 10000; ix1++)
        aaShort[ix1] = ix1;
    //_aaDebug( &aaShort,true);
    for(short x2 = 100; x2 <= 10000; x2++)
    {
		short val = aaShort[x2];

        if (val != x2)
        {
            test_failed(text("index error ",x2," got ", val));
        }
    }
    aaShort.clear;

    HashTable!(string[string]) aaString;

    string[] skey = getStringSet(20,1000);

    foreach(s ; skey)
        aaString[s] = s;

    foreach(s ; skey)
        if (aaString[s] != s)
            writeln("index error ",s);

    string[string] ss_copy = aaString.AA;

    foreach(k,v ; ss_copy)
    {
        auto p = k in aaString;
        if (p is null)
        {
            test_failed("alias key not found");
            break;
        }
    }
    int test_key(ref string s)
    {
        auto p = s in ss_copy;
        if (p is null)
        {
            test_failed("alias key not found");
            return 1;
        }
        return 0;
    }
    aaString.eachKey(&test_key);

    foreach(s ; skey)
    {
        string check;
        if (!aaString.remove(s, check))
        {
            test_failed("remove key not found");
            break;
        }
        if (check != s)
        {
            test_failed("remove value mismatch");
        }
    }

    HashSet!(string)   hset;

    hset["super"] = true;

    if (!hset.contains("super"))
        test_failed("hashset key not found");

    hset["super"] = false;
    if (hset.contains("super"))
        test_failed("hashset key should not be found");

    foreach(s ; skey)
        hset.put(s);

    foreach(s ; skey)
    {
        if (!hset.contains(s))
        {
            test_failed("hashset key not found");
            break;
        }
    }
    string[] copykeys = hset.keys;

    if (hset.length != copykeys.length)
        test_failed("keys length not equal");

    foreach(s ; copykeys)
        hset.remove(s);

    if (hset.length != 0)
        test_failed("hash set length not zero");
    writeln("Unit tests done");

    HashTable!(Variant[Variant]) aaVariant;

    Variant x;
    Variant s;

    Variant z;

    x = 100;
    //s = "hello";
    //z = 123.4;
/*
    aaVariant[x] = s;
    aaVariant[s] = x;

    aaVariant[z] = s;
    aaVariant[s] = z;

    aaVariant[x] = z;
    aaVariant[z] = x;
*/
}
