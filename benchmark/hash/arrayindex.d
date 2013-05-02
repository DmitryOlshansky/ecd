/**
Array indexing implementation, and derived hash map. 
Instead of using pointers to nodes, it has a separate arrays for
hash values, and array of double linked integer indexes,
and a map of the hash to link index.  In theory, each of these should be treated by the D
memory management has have no garbage collectable pointers. 
Arrays for Keys and Values will depend on the type provided by the templates.

It can handle duplicate key entries, and unless deletions are followed by insertions,
it will iterate in the order of insertion. 

If no freelinks then the arrays are compact, without holes.
Empty slots are marked by setting the
Hibit in the hash array entry, and adding the associated index link record to the freelinks.
Links will be chained from hash collisions and duplicates of course.

After index removal, keys and values remain at same index until replaced by a further insertion.

The KeyIndex template, and the indexArray method can be used to index an existing array in place.

Performance gets slower than the builtin druntime hash map implementation. On
reaching larger array sizes of 1_000_000,it times about 25% slower.
The builtin AA does allocate memory nodes incrementally which
only need to be relinked when the main hash table grows, whereas this implementation has to reallocate
all of the arrays each time array capacity is exceeded. Excess reallocation can be avoided by preset call to capacity,
but its still a little slower, but useable at larger array sizes.

Performance of KeyIndex will degrade if lots of duplicate keys. 
The HashMap implementation maintains unique keys with findPut.

With separate arrays and use of integers instead of pointers to aggregated nodes,
it may be a little more immune to garbage collection issues that druntime AA can get.

Not entirely immune however, as "memory allocation failure" exception did occur,
when repeatedly tested with large arrays string keys, when a development bug meant 
that key and value arrays where being incremented by length, instead of capacity managed.

The idea and the code were adapted from the "Ultimate++" sources Index.h, Index.hpp Index.cpp,
and reworked in D idiom, with a reduction in the number functions by inlining, and reducing the 
code to the basic concepts necessary to support a KeyIndex, and KeyValueIndex templates.

The Ultimate++ has an overall BSD license, but this implementation is no longer in C++, nor have I been
able to find other similar implementations.

*/

/**
Authors: Michael Rynn, michaelrynn@optusnet.com.au
Date: October 14, 2010

 */


module hash.arrayindex;

import hash.util;
import std.variant;
import std.array;
import std.stdio;

//version=FOLDHASH;
/** 
	Insertion into a full array always results in incrementing the length
    of the hash_,  links_,  keys_,  values_ array.
	The insertion index is always shared between hash_, links_, keys_, values_.
	If a free link exists, then the index of that is used.
	
	The length_ indicates the number of occupied slots.
	If the length_ == hash_.length, then the arrays are full.
	Use capacity to help insertion times
*/
alias void delegate(uint newcap) SetCapDg;
alias void delegate(uint unsetIndex) IndexRemoveDg;

struct HashLinkMap {
	struct HLink {
		int	next;
		int	prev;
	}	
	uint		capacity_; // reserved capacity for values
	uint		mapcap_;  // map loading capacity
	uint[]		hash_;		// remember the hash values
	int[]		hmap_;
	HLink[]		hlinks_;
	int			freelinks_ = -1;
	double      loadRatio_ = 0.8;
	uint		length_;   // active links
	SetCapDg	capdg_;
	IndexRemoveDg	removeDg_;
	
	enum { UNSIGNED_HIBIT = 0x80000000 }
	
version(FOLDHASH)
	static uint hashBound(uint i) { return getNextPower2(i); }
else
	static uint hashBound(uint i) { return getNextPrime(i); }
	
	/** For pre-emptive strike on memory */

	
	@property size_t length() { return length_; }
	
	// return index and final form of hash to put in hash_
	
	int findLink(uint h)
	{
		invariant hlen = hmap_.length;
		h = h & ~UNSIGNED_HIBIT;
		if(hlen == 0) 
			return -1;
		version(FOLDHASH)
			return hmap_[(hlen - 1) & (((h >> 23) - (h >> 15) - (h >> 7) - h))];
		else
			return hmap_[h % hlen];
	}
	
	void assign(ref HashLinkMap hlm)
	{
		hlinks_ = hlm.hlinks_.dup;
		hmap_ = hlm.hmap_.dup;
		hash_ = hlm.hash_.dup;
		freelinks_ = hlm.freelinks_;
		loadRatio_ = hlm.loadRatio_;
		length_ = hlm.length_;
	}
	
	int findNext(int i)
	{
		int q = hlinks_[i].next;
		invariant hlen = hmap_.length;
		uint h = hash_[i];
		version(FOLDHASH)
			uint ix = hmap_[(hlen - 1) & (((h >> 23) - (h >> 15) - (h >> 7) - h))];
		else
			uint ix = hmap_[h % hlen];
		return (q == ix) ? -1 : q;
	}
	
	void unlink(int i)
	{
		
		assert(i < hash_.length);
		
		uint h = hash_[i];
		assert((h & UNSIGNED_HIBIT) == 0);
		hash_[i] = h | UNSIGNED_HIBIT;
		length_--;
		if(i < hlinks_.length) 
		{ // TODO: what about earlier check?
			HLink* h0 = &hlinks_[i];
			invariant hlen = hmap_.length;
			version(FOLDHASH)
				int* mptr = &hmap_[(hlen - 1) & (((h >> 23) - (h >> 15) - (h >> 7) - h))];
			else
				int* mptr = &hmap_[h % hlen];
			
			if(i == *mptr) {
				if(h0.next == i) {
					*mptr = -1;
					return;
				}
			}
			// unlink
			*mptr = h0.next;
			hlinks_[h0.next].prev = h0.prev;
			hlinks_[h0.prev].next = h0.next;
			
			// link to freelinks_;
			if(freelinks_ >= 0) {  // already linked to another value
				HLink* h2 = &hlinks_[freelinks_];
				h0.next = freelinks_;
				h0.prev = h2.prev;
				h2.prev = i;
				hlinks_[h0.prev].next = i;
			}
			else {
				freelinks_ = h0.prev = h0.next = i;
			}
			if (removeDg_)
				removeDg_(i);
		}
	}
	
	/** the link and hash index is given, so just link it */
	void setLinkIndex(int f0, uint hash)
	{
		invariant h = hash & ~UNSIGNED_HIBIT;
		invariant hlen = hmap_.length;
		hash_[f0] = h;
		length_++;
		version(FOLDHASH)
			int* mptr = &hmap_[(hlen - 1) & (((h >> 23) - (h >> 15) - (h >> 7) - h))];
		else
			int* mptr = &hmap_[h % hlen];
		invariant mval = *mptr;
		HLink* ink = &hlinks_[f0];
		if(mval >= 0) {  // already linked to another value
			HLink* h2 = &hlinks_[mval];
			ink.next = mval;
			ink.prev = h2.prev;
			h2.prev = f0;
			hlinks_[ink.prev].next = f0;
		}
		else {
			*mptr = ink.prev = ink.next = f0;
		}
	}
	
	

	/** make or get unused link for new hash */
	int makeLink(uint hash)
	{
		uint hlen = hmap_.length;
		uint hsize = hash_.length;

		int f0 = freelinks_;
		if (f0 >= 0)
		{
			HLink* ink = &hlinks_[f0];
			freelinks_ = ink.next;
			if (f0 == freelinks_)
				freelinks_ = -1;
			else {
				hlinks_[ink.next].prev = ink.prev;
				hlinks_[ink.prev].next = ink.next;				
			}
		}
		else 
		{
			// are we full yet
			if (hsize+1 >= capacity_)
			{
				if (hsize == 0)
					hsize = 4;
				if (capdg_ !is null)
					capdg_(hsize*2);
				else
					capacity(hsize*2);
				//reindex(hsize+1);
			}		
			else if (length_ +1 >= mapcap_)
			{
				reindex((length_ +1)*2);
			}
			
			f0 = hlinks_.length;
			hash_ ~= f0;
			hlinks_ ~= HLink(f0,f0);
					
		}
		setLinkIndex(f0, hash);
		return f0;
	}
	
	/** clear everything, maybe setup again */
	void clear()
	{
		hlinks_ = null;
		hmap_ = null;
		freelinks_ = -1;
		length_ = 0; 
		hash_ = null;
		capacity_ = 0;
		mapcap_ = 0;
	}	
	
	void capacity(uint cap)
	{
		if (cap > 0)
		{
			capacity_ = hlinks_.reserve(cap);
			//debug {
				
			//}
			uint ncap = hash_.reserve(cap);		
			if (ncap < capacity_)
				capacity_ = ncap;
			//if (hash_.length > 0)
				reindex(cap);
		}
	}
	/** remap the hash values to links */
	private void reindex(int n)
	{
		hlinks_.length = 0;
		freelinks_ = -1;
		//length_ = 0;   length_ does not change during this process == number of valid hash_[]
		
		size_t nlen = cast(size_t)(n / loadRatio_);
		hmap_.length = hashBound(nlen);
		mapcap_ = cast(size_t) (loadRatio_ * hmap_.length);
		hmap_[] = -1; 
		finishIndex();
	}
	
	/** remap links  from hash_ to hlinks_ */
	private void finishIndex()
	{
		uint llen = hlinks_.length;
		uint hsize = hash_.length;
		if (llen < hsize)
		{
			hlinks_.length = hsize;
			invariant hlen = hmap_.length;
			int* mptr = void;
			for(uint i = llen; i < hsize; i++)
			{
				uint h = hash_[i]; // convert hash into map index
				if ((h & UNSIGNED_HIBIT)==0)
				{				
					version(FOLDHASH)
						mptr = &hmap_[(hlen - 1) & (((h >> 23) - (h >> 15) - (h >> 7) - h))];
					else
						mptr = &hmap_[h % hlen];
				}
				else
					mptr = &freelinks_;
				invariant mval = *mptr;
				HLink* f1 = &hlinks_[i]; // mapped to link with index i
				if(mval >= 0) {
					HLink* f2 = &hlinks_[mval];
					f1.next = mval;
					f1.prev = f2.prev;
					f2.prev = i;
					hlinks_[f1.prev].next = i;
				}
				else {
					*mptr = f1.prev = f1.next = i;
				}
			}
		}
	}
}

/**
	Usage:
----
	string[] test = getStringSet(40,10_000);
	scope indx = new KeyIndex!(string[]);
	indx.indexArray(test);
	
	string lookForAll = "random hello";
	int i = indx.findKeyIndex(lookForAll);
	if (i >= 0)
	{
		// anymore?
		i = indx.nextKeyIndex(lookForAll,i);
	}
----
*/
class KeyIndex(K : K[])
{
	protected {
		K[]	keys_;
		HashLinkMap	hlm_;
		TypeInfo    keyti_;	
		
		/** Remove all entries for the key */
		final int unlinkKey(ref K k, uint h)
		{
			int n = 0;
			int q = hlm_.findLink(h);
			while(q >= 0)
			{
				int w = q;
				q = hlm_.findNext(q);
				if(k == keys_[w])
				{
					hlm_.unlink(w);
					n++;
				}
			}
			return n;
		}
		/**  always adds a new entry */
		final int putHash(ref K k, uint _hash)
		{
			int q = hlm_.makeLink(_hash);
			if (q >= keys_.length)
				keys_.length = q+1;
			keys_[q] = k;
			return q;
		}
		final int findKeyHash(ref K k, uint _hash)
		{
			
			int i = hlm_.findLink(_hash);
			while(i >= 0 && !(k == keys_[i]))
				i = hlm_.findNext(i);
			return i;
		}
	}
		
	this(KeyIndex ki)
	{
		keys_ = ki.keys_.dup;
		keyti_ = ki.keyti_;
		hlm_.assign(ki.hlm_);
		hlm_.capdg_ = &capacity;
	}
	
	this(K[] k)
	{
		keyti_= typeid(K);
		hlm_.capdg_ = &capacity;	
		indexArray(k);
	}
	this()
	{
		keyti_= typeid(K);
		hlm_.capdg_ = &capacity;
	}
	
	void capacity(uint cap)
	{
		hlm_.capacity(cap);
		uint ncap = keys_.reserve(cap);
		if (ncap < hlm_.capacity_)
			hlm_.capacity_ = ncap;
	}
	
	// no entries are unlinked, no holes in key, values, hash_
	bool isCompact()
	{
		return (hlm_.freelinks_ == -1);
	}
	
	@property uint length()
	{
		return hlm_.length_;
	}
	
    @property double loadRatio()
    {
        return hlm_.loadRatio_;
    }

	// May be useful with key removal to treat keys and values
	void OnIndexRemoval(IndexRemoveDg dg)
	{
		hlm_.removeDg_ = dg;
	}
	
    @property void loadRatio(double ratio)
    {
		hlm_.loadRatio_ = ratio;
    }	
	final const(uint)[] hashData()
	{
		return 	hlm_.hash_;
	}				
	// alias to and hash index the array
	final void indexArray(K[] ka)
	{
		keys_ = ka;
		hlm_.clear();
		uint ilen = keys_.length;
		capacity(ilen);
		hlm_.hash_.length = ilen;
		hlm_.hlinks_.length = ilen;
		for(uint i = 0; i < keys_.length; i++)
		{
			// assume duplicates are possible?
			// in this process, the link record is fixed by key position
			uint h = keyti_.getHash(&keys_[i]);
			hlm_.setLinkIndex(i, h);
		}
	}
		/*
		uint h = keyti_.getHash(&k);
		int ix = findKeyHash(k, h);
		if (ix >= 0)
		{
			keys_[ix] = k;
			fix = ix;
			return true;
		}
		else {
			fix = putHash(k, h);
			return false;
		}
		*/	
	// looks for first of any existing entries, and replaces key
	// return true if existing key
	final bool findPut(ref K k, ref int fix)
	{
		uint h = keyti_.getHash(&k);
		int startLink = hlm_.findLink(h);
		if (startLink >= 0)
		{
			// collision or key match chain found.
			int ix = startLink;
			do
			{
				if (keys_[ix] == k)
				{
					// key is already inserted at ix
					fix = ix;
					return true;
				}
				ix = hlm_.hlinks_[ix].next;
			}
			while (ix != startLink);
		}
		int q = hlm_.makeLink(h);
		if (q >= keys_.length)
			keys_ ~= k;
		else
			keys_[q] = k;
		fix = q;
		return false;
		/*
		uint h = keyti_.getHash(&k);
		int ix = findKeyHash(k, h);
		if (ix >= 0)
		{
			keys_[ix] = k;
			fix = ix;
			return true;
		}
		else {
			fix = putHash(k, h);
			return false;
		}
		*/
	}

	final bool put(K k)
	{
		int ix = -1;
		return findPut(k, ix);
	}
	
	final int putdup(ref K k)
	{
		return putHash(k, keyti_.getHash(&k));
	}	
	
	final int putdup(K k)
	{
		return putHash(k, keyti_.getHash(&k));
	}		
		
	final int findKeyIndex(ref K k)
	{
		return findKeyHash(k, keyti_.getHash(&k));
	}
		
	final int nextKeyIndex(ref K k, int i)
	{
		while(i >= 0 && !(k == keys_[i]))
			i = hlm_.findNext(i);
		return i;			
	}

	final bool contains(K k)
	{
		return  findKeyHash(k, keyti_.getHash(&k)) >= 0 ? true : false;
	}
	
	final int removeKey(K k)
	{
		return unlinkKey(k, keyti_.getHash(&k));	
	}		
	
	final int removeKey(ref K k)
	{
		return unlinkKey(k, keyti_.getHash(&k));	
	}
		
	final void clearKeys()
	{
		hlm_.clear();
		keys_ = null;
	}
	
	final void removeIndex(int i)
	{
		hlm_.unlink(i);
	}
}

void test_index()
{
	auto a = [1,2,3,4,5,6];
	replace(a,4,5,null);
	
	string[] test = getStringSet(40,10_000);
	scope indx = new KeyIndex!(string[]);
	indx.indexArray(test);
	for(int i = 0; i < test.length; i++)
	{
		if (! (indx.findKeyIndex(test[i]) == i) )
			throw new AAError("unittest for KeyIndex(string[]) failed");
	}
}

/** 

	Usage:
---

	auto hm = new KeyValueIndex!(int[string]);

	// code like an AA, its also a KeyIndex

	hm["test1"] = 1;
	
	// add duplicates

	hm.putdup("test1") = 2;

	// access raw data
	auto keys = hm.keyData();
	auto values = hm.valueData();  // danger, aliased!

	// remove all copies of a key

---
*/
class KeyValueIndex(V : V[K], K ) : KeyIndex!(K[]) 
{
	V[]  values_;
		
	
	this(KeyValueIndex kvi)
	{
		// duplicate everything
		super(kvi);
		values_ = kvi.values.dup;
		hlm_.capdg_ = &capacity;
	}

	this()
	{
		super();
		hlm_.capdg_ = &capacity;
		
	}
	
	
	final int putdup(ref K k, ref V v)
	{
		int ix = super.putHash(k, keyti_.getHash(&k));
		if (ix >= values_.length)
		{
			values_.length = ix+1;
		}
		values_[ix] = v;
		return ix;
	}	
		
	final V get(ref K k)
	{
		int ix = super.findKeyIndex(k);
		if (ix >= 0)
			return values_[ix];
		else 
			throw new AAError("V get(key) failed");
	}
	uint capacity()
	{
		return hlm_.capacity_; // may not be strictly true
	}
	
	void capacity(uint cap)
	{
		
		super.capacity(cap);
		uint ncap = values_.reserve(cap);
		if (ncap < hlm_.capacity_)
			hlm_.capacity_ = ncap;
		//writeln("Reserved ", cap, " got ", hlm_.capacity_);
	}
	
	final V get(K k)
	{
		return get(k);
	}
	
	final void clear()
	{
		super.clearKeys();
		values_ = null;
	}
	
	final void rehash()
	{
		hlm_.reindex(hlm_.length_);	
	}
	
	final bool put(K k, V v)
	{
		int ix = -1;
		bool result = super.put(k);
		values_[ix] = v;
		return result;
	}
	
	final int putdup(K k, V v)
	{
		int ix = super.putHash(k, keyti_.getHash(&k));
		if (ix >= values_.length)
		{
			values_.length = ix+1;
		}
		values_[ix] = v;
		return ix;
	}		

	final V* opIn_r(K k)
	{
		//wrap
		int ix = super.findKeyIndex(k);
		if (ix >= 0)
			return &values_[ix];
		else 
			return null;
	}

    final V opIndex(K k)
    {
		int ix = super.findKeyIndex(k);
		if (ix >= 0)
			return values_[ix];
		else 
			throw new AAError("V get(key) failed");
    }
	
	final bool get(K k, ref V val )
	{
		int ix = super.findKeyIndex(k);
		if (ix >= 0)
		{
			val = values_[ix];
			return true;
		}
		static if(!is(V==Variant))
			val = V.init;
		return false;		
				
	}
    final bool remove(K key)
    {
		int ix = super.findKeyIndex(key);
		if (ix >= 0)
		{
			removeIndex(ix);
			return true;
		}
		return false;
    }
	
    final bool remove(K key, ref V value)
    {
		int ix = super.findKeyIndex(key);
		if (ix >= 0)
		{
			value = values_[ix];
			removeIndex(ix);
			return true;
		}
		return false;
    }
	
	final void opIndexAssign(V value, K k)
	{	
		int ix = -1;
		if (!super.findPut(k, ix))
		{
			if (ix >= values_.length)
			{
				values_ ~= value;
				return;
			}
		}
		values_[ix] = value;
	}	
   
	@property K[] keyData()
	{
		return keys_;
	}
	
	@property V[] valueData()
	{
		return values_; 
	}
	
	/** get a copy? of all the current keys */
	@property K[] keys()
	{
	    if (super.length == keys_.length)
            return keys_.dup;
		else
		{
			K[] nkey;
			nkey.length = super.length;
			const(uint)[] hvalues = super.hashData();
			uint ct = 0;
			for(uint i = 0; i < hvalues.length; i++)
			{
				if ((hvalues[i] & HashLinkMap.UNSIGNED_HIBIT)==0)
					nkey[ct++] = keys_[i];
			}
			return nkey;
		}
	}

	int forEachValue(int delegate(V value) dg)
	{
		if (hlm_.length_ == values_.length)
		{
			for(uint i = 0; i < hlm_.length_; i++)
			{
				int result = dg(values_[i]);
				if (result)
					return result;
			}
		}
		else {
			const(uint)[] hvalues = super.hashData();			
			for(uint i = 0; i < hlm_.length_; i++)
			{
				if ((hvalues[i] & HashLinkMap.UNSIGNED_HIBIT)==0)
				{	
					int result = dg(values_[i]);
					if (result)
						return result;
				}
			}	
		}
		return 0;
	}
	
    @property
	V[] values()
	{
	    if (hlm_.length_ == values_.length)
            return values_.dup;
		else
		{
			V[] nval;
			nval.length = hlm_.length_;
			const(uint)[] hdata = super.hashData();
			uint ct = 0;
			for(uint i = 0; i < hdata.length; i++)
			{
				if ((hdata[i] &  HashLinkMap.UNSIGNED_HIBIT)==0)
					nval[ct++] = values_[i];
			}
			return nval;
		}
	}	
	int opApply(int delegate(ref V value) dg)
	{
		const(uint)[] hvalues = super.hashData();
		uint ct = 0;
		for(uint i = 0; i < hvalues.length; i++)
		{
			if ((hvalues[i] &  HashLinkMap.UNSIGNED_HIBIT)==0)
			{
				int result = dg(values_[i]);
				if (result)
					return result;
			}
		}
		return 0;
	}

	int opApply(int delegate(ref K key, ref V value) dg)
	{
		const(uint)[] hvalues = super.hashData();
		uint ct = 0;
		for(uint i = 0; i < hvalues.length; i++)
		{
			if ((hvalues[i] &  HashLinkMap.UNSIGNED_HIBIT)==0)
			{
				int result = dg(keys_[i], values_[i]);
				if (result)
					return result;
			}
		}
		return 0;
	}	
       /// 0: table length, 1 : unoccupied nodes,
        /// 2* : followed by [n-2] :  number of nodes of length n
	/// ignoring duplicate keys, only interested in length of chains on a hash
	/// In going through hash array, will hit common chains.
	/// in order to handle this, identify each chain by its lowest link number, store count
	/// this will repeat some counting
    @property uint[] list_stats()
    {
		int lowlink[];
		uint[] result;
		result.length = 16;
		
		lowlink.length = hlm_.hlinks_.length;
		lowlink[] = -1;

		int[] hdata = hlm_.hmap_;
		result[0] = hdata.length;
		
		int emptybuckets = 0;
		HashLinkMap.HLink[] links = hlm_.hlinks_;
		
		for(uint i = 0; i < hdata.length; i++)
		{
			int ix = hdata[i];
			if (ix != -1)
			{
				int firstLink = ix;
				int chainct = 1;
				int minlink = firstLink;
				ix = links[ix].next;
				while (ix != firstLink)
				{
					chainct++;
					if (ix < minlink)
						minlink = ix;
					ix = links[ix].next;
				}
				if (lowlink[minlink] == -1)
				{
					lowlink[minlink] = chainct;
					if (chainct >= result.length-1)
				    {
                    result.length = chainct + 2;
					}
					result[chainct+1] += 1;
				}
			}
			else 
				emptybuckets++;
		}
        result[1] = emptybuckets;
        return result;
    }
}


struct HashTable(V : V[K], K)
{
    alias KeyValueIndex!(V[K])  AAClassImpl;

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
			return imp_.opIndex(k);
        }
        throw new AAKeyError("no key for opIndex");
    }
	void opIndexAssign(V value, K k)
	{
	    if (imp_ is null)
            imp_ = new AAClassImpl();
        imp_.opIndexAssign(value, k);
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
    @property HashTable init()
    {
        HashTable result;
        return result;
    }
    @property HashTable allocate()
    {
        HashTable result;
        result.imp_ = new AAClassImpl();
        return result;
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
		if (imp_ !is null)
		{
			return imp_.get(k);
		}
        throw new AAKeyError("get on absent AA key");
    }

	bool get(K k, ref V val )
	{
	    if (imp_ !is null)
	    {
           return imp_.get(k, val);
	    }
		static if(!is(V==Variant))
			val = V.init;
		return false;
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
        return imp_ is null ? false
			  : imp_.contains(k);
	}
	
    @property void capacity(size_t cap)
    {
	    if (imp_ is null)
            imp_ = new AAClassImpl();
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
            imp_ = new AAClassImpl();
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

    }
    @property uint[] list_stats()
    {
        if (imp_ is null)
            return null;
        //'return imp_.used;
        return imp_.list_stats;
    }
	@property size_t length()
	{
	    if (imp_ is null)
            return 0;
        //'return imp_.used;
        return imp_.length;
	}


    @property HashTable dup()
    {
        HashTable copy;

        if (imp_ !is null)
        {
            copy.imp_ = new AAClassImpl(imp_);
        }
        return copy;
    }
    bool remove(K key, ref V value)
    {
        if (imp_ !is null)
            return imp_.remove(key, value);
		else
			return false;
    }
    void remove(K key)
    {
        if (imp_ !is null)
            imp_.remove(key);
    }
	 // return true if new key inserted
    bool put(K k, ref V value)
    {
		if (imp_ is null)
            imp_ = new AAClassImpl();
		int ix = -1;
		bool result = imp_.findPut(k, ix);
		return imp_.put(k, value);
    }	
	int forEachValue(int delegate(V value) dg)
	{
		return (imp_ !is null) ? imp_.forEachValue(dg) : 0;
	}
	
	int opApply(int delegate(ref V value) dg)
	{
		return (imp_ !is null) ? imp_.opApply(dg) : 0;
	}

	int opApply(int delegate(ref K key, ref V value) dg)
	{
        return (imp_ !is null) ? imp_.opApply(dg) : 0;
	}
}
