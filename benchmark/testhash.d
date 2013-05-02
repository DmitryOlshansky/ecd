/**
    Demonstrate failings of AA garbage collection.
    Based on test.d for PyDict.

    Michael Rynn

*/

module testhash;


import core.memory;
import std.perf;
import std.conv;
import std.exception;
import std.random;
import std.stdio;
import std.stream;
import std.math;
import hash.pydict;
import hash.aatree;
import hash.hashptr;
import hash.hashlist;
import hash.hashtable;
//import rt.util.hash;
import hash.arrayindex;

import hash.lookup;

//extern(C) long _aaStats(void*);



import hash.lookup;

import hash.util;

import std.algorithm;
//import alt.ustring;

import randAA.RandAA;

import tangy.util.container.Container;
import tangy.util.container.HashMap;

//private import gc.gc;
//private import gc.gcstats;
version=RandomValue;

//version=ClearAA;
//version=PreCollect;

// do a standard task to relativize timings
// maybe make more independent of CPU speed and current background activity.
// Since the time for this should be relatively constant, measuring this
// also gives an indication of current CPU interference, and writes to memory.
// Ideally the task should a good fraction of a second, to be a valid sample of CPU speed
// and availability.

void relativeTimeTask()
{
    uint buf[1024];

    for(uint i = 0; i < 1_000_000; i++)
    {
        buf[i % buf.length] = rndGen.front;
        rndGen.popFront;
    }
    for(uint i = 0; i < 1_000_000; i++)
    {
        uint swapix1 = uniform(0, buf.length, rndGen);
        uint swapix2 = uniform(0, buf.length, rndGen);
        if (swapix2 != swapix2)
        {
            uint temp = buf[swapix1];
            buf[swapix1]  = buf[swapix2];
            buf[swapix2] = temp;
        }
    }
}
struct ArrayInputRange( T : T[] ) {
    private T[][]  data_;

    this(T[][] data)
    {
        data_ = data;
    }

    bool empty()
    {
        return  data_.length == 0;
    }

    void popFront()
    {
        data_ = data_[1..$];
    }

    T[] front()
    {
        assert(data_.length > 0);
        return data_[0];
    }

}
class TestException : Exception {
    this(string msg) { super(msg); }
}



/// template to remove all keys (nodes) from builtin AA type

void
clear(K,V)(ref V[K] aa)
{
    K[] keyset = aa.keys;
    foreach(ks; keyset)
    {
        aa.remove(ks);
    }
}







    template test_drhm(K,V) {
        alias hash.lookup.HashTable!(V[K]) AAS;

        AAS aa;

        void allocate(ref AAS aab)
        {
            // do nothing
        }
        void loadRatio(ref AAS aab, double ratio)
        {
            aab.loadRatio = ratio;
        }
        void capacity(ref AAS aab, size_t cap)
        {
            aab.capacity = cap;
        }
        void aa_cleanup(){
            aa.clear;
        }
    }

    template test_builtin(K,V) {
        alias V[K] AAS;

        AAS aa;

        void allocate(ref AAS aab)
        {
            // do nothing
        }
        void loadRatio(ref AAS aab, double ratio)
        {
            //aab.loadRatio = ratio;
        }
        void capacity(ref AAS aab, size_t cap)
        {
            //aab.capacity = cap;
        }
        void aa_cleanup(){
            // assist the GC
            K[] keys = aa.keys;
            foreach(aa_key ; keys)
                aa.remove(aa_key);
        }

        uint[] list_stats(AAS aab)
        {
			return new uint[2];
			/*
            long sa = _aaStats(cast(void*) aab);
            uint[] result = *(cast(uint[]*)&sa);
            return result;
			*/
        }
    }
template test_aahm(K,V) {
    alias hash.arrayindex.HashTable!(V[K])    AAS;
    AAS aa;

    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }

    void aa_cleanup(){
        aa.clear;
    }
}
template test_aahm1(K,V) {
    alias hash.hashtable.HashTable!(V[K])    AAS;
    AAS aa;

    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }

    void aa_cleanup(){
        aa.clear;
    }
}
template test_aaht_nogc(K,V) {
    alias aaht!(K,V, Container.Chunk)    AAS;
    AAS aa;

    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }

    void aa_cleanup(){
        aa.clear;
    }
}

template test_aahtl(K,V) {
    alias aahl!(K,V)    AAS;
    AAS aa;
    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }
    void aa_cleanup(){
        aa.clear;
    }
}
template test_aahl(K,V) {
    alias aahl!(K,V)    AAS;
    AAS aa;
    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        //aab.capacity = cap;
    }
    void aa_cleanup(){
        aa.clear;
    }
}

template test_aahp(K,V) {
    alias aahp!(K,V)    AAS;
    AAS aa;

    void allocate(ref AAS aab)
    {
        aab = aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }
    void aa_cleanup(){
        aa.clear;
    }
}

template test_aaht(K,V) {
    alias aaht!(K,V)    AAS;
    AAS aa;

    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }

    void aa_cleanup(){
        aa.clear;
    }
}

template test_aaprlpy(K,V) {
    alias aard!(K,V)    AAS;
    AAS aa;

    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }
    void aa_cleanup(){
        aa.clear;
    }
}
template test_aaprlrd(K,V) {
    alias aard!(K,V,true)    AAS;
    AAS aa;

    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }
    void aa_cleanup(){
        aa.clear;
    }
}

template test_PyDict(K,V) {
    alias PyDict!(K,V)  AAC;
    auto aa = new AAC();
    void allocate(ref AAS aab)
    {
        aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }
    void aa_cleanup(){
        aa.clear;
    }

}
template test_aapy(K,V) {
    alias aapy!(K,V)   AAS;
    AAS aa;

    void allocate(ref AAS aab)
    {
        aab = aa.allocate;
    }

    void loadRatio(ref AAS aab, double ratio)
    {
        aab.loadRatio = ratio;
    }
    void capacity(ref AAS aab, size_t cap)
    {
        aab.capacity = cap;
    }
    void aa_cleanup(){
        aa.clear;
    }
}


struct runOptions {
    bool   aaClear = true;
    bool   noGC = false;
	bool   noCap = false;
    bool   postCheck = false;
    bool   randomLookup = true;
    bool   generate = false;
    bool   readGen = false;
    bool   normalizeTime = true;

    double loadRatio = 0.0;

    string  dataFile;

} 

/// a stream of names/keys (string) and associated data
//alias UString WrapString;
bool gShowData;

class WrapString {
    char[] str_;
    hash_t  hash_;

    this(){}

    private void rehash()
    {
		//hash_ = hashOf(str_.ptr, str_.length, str_.length);
        //hash_ = superHash(str_);
        hash_ = typeid(str_).getHash(&str_);
		/*hash_t temp = str_.length;
        for(uint i = 0; i < str_.length; i++)
            temp = temp * 31 + str_[i];
		hash_ = temp; */
		
		
		

    }
    this(string s)
    {
        str_ = s.dup;
        rehash();
    }

    hash_t toHash()
    {
		rehash();
        return hash_;
        //return typeid(str_).getHash(&str_);
    }
    int opCmp(Object o)
    {
        WrapString ustr = cast(WrapString)o;
        if (ustr is null)
            return 1;
        int cpresult = typeid(str_).compare(&str_,&ustr.str_);
        if (gShowData)
        {
            writefln("opCmp %s %s r = %d hash %x %x", str_, ustr.str_,cpresult,
                     hash_, ustr.hash_);
        }
        return cpresult;
    }
    equals_t opEquals(Object o)
    {
        WrapString c = cast(WrapString)o;
        if (c is null)
            return false;
        if (c.hash_ != this.hash_)
            return false;
        return (this.str_ == c.str_);
    }
    string toString()
    {
        return str_.idup;
    }

    void read(InputStream ins)
    {
        size_t slen;
        ins.read(slen);

        char[] temp = new char[slen];
        ins.readExact(temp.ptr, char.sizeof * slen);
        str_ = temp;
        rehash();
        //str_ = assumeUnique(temp);
    }
    void write(OutputStream outs)
    {
        size_t slen = str_.length;
        outs.write(slen);
        outs.writeExact(str_.ptr, char.sizeof * slen);
    }
}
class WrapUint {
    uint val_;
    hash_t  hash_;

    this(){}

    this(uint s)
    {
        val_ = s;
    }

    void rehash()
    {
        int key = val_;
        key = ~key + (key << 15); // key = (key << 15) - key - 1;
        key = key ^ (key >>> 12);
        key = key + (key << 2);
        key = key ^ (key >>> 4);
        key = key * 2057; // key = (key + (key << 3)) + (key << 11);
        key = key ^ (key >>> 16);
        hash_ = key;
    }

    hash_t toHash()
    {
        return hash_;
    }
    override int opCmp(Object o)
    {
        WrapUint c = cast(WrapUint)o;
        if (c is null)
            return -1;

        return (this.val_ - c.val_);
    }

    equals_t opEquals(Object o)
    {
        WrapUint c = cast(WrapUint)o;
        if (c is null)
            return false;

        return (this.val_ == c.val_);
    }
    string toString()
    {
        return to!(string)(val_);
    }

    void read(InputStream ins)
    {

        ins.readExact(&val_,uint.sizeof);
        rehash();
        //str_ = assumeUnique(temp);
    }
    void write(OutputStream outs)
    {
        outs.writeExact(&val_,uint.sizeof);
    }
}

struct CheckedOutputStream {

      OutputStream os;
      this(OutputStream outs)
       {
           os = outs;
       }

     void writeString(string s)
     {
         size_t slen = s.length;
         os.write(slen);
         os.writeExact(s.ptr, char.sizeof * slen);
     }
     void writeBool(bool val)
     {
         os.write(cast(ubyte)val);
     }

     void writeUint(uint val)
     {
         os.write(val);
     }

     void writeInt(int val)
     {
         os.write(val);
     }
     void writeArray(uint[] val)
     {
         os.write(val.length);
         os.writeExact(val.ptr,val.length * uint.sizeof);
     }
     void writeArray(string[] val)
     {
         os.write(val.length);
         foreach(s ; val)
         {
            os.write(s.length);
            os.writeString(s);
         }

     }
     void writeArray(WrapString[] val)
     {
         os.write(val.length);
         foreach(cs ; val)
         {
            cs.write(os);
         }

     }
}

struct CheckedInputStream {

    InputStream ins;

    this(InputStream inStream)
    {
        ins = inStream;
    }

    void readString(out string s)
    {
        size_t slen;
        ins.read(slen);
        char[] key = ins.readString(slen);
        s =  assumeUnique(key);
    }
    void readBool(out bool val)
    {
        ubyte  bval;
        ins.read(bval);
        val = (bval == 0) ? false : true;
    }

    void readUint(out uint val)
    {
        ins.read(val);
    }

    void readInt(out int val)
    {
        ins.read(val);
    }


    void readArray(out uint[] val)
    {
        size_t alen;
        ins.read(alen);
        val = new uint[alen];
        ins.readExact(val.ptr,alen * uint.sizeof);
    }
    void readArray(out string[] val)
    {
        size_t alen;
        ins.read(alen);
        val = new string[alen];
        for(uint i = 0; i < alen; i++)
        {
            size_t slen;
            ins.read(slen);
            char[] s = ins.readString(slen);
            val[i] = assumeUnique(s);
        }
    }
}

struct NormalStatistic {
    uint samples;
    double mean;
    double stddev;
}

void getStatistic(double values[], out NormalStatistic stats)
{
    real sum = 0;
    stats.samples = values.length;

    if (stats.samples < 1)
        return;

    foreach(v ; values)
        sum += v;

    stats.mean = sum / stats.samples;

    if (stats.samples < 2)
        return;

    real sumVar = 0;
    foreach( v ; values)
    {
        double sq = (stats.mean - v);
        sumVar += sq*sq;
    }
    stats.stddev = sqrt(sumVar / (stats.samples-1));

}
class RunStats {
    uint[]   rand_index;

    PerformanceCounter  timer;
    double[] insert_times;
    double[] lookup_times;
    double[] clear_times;
    double post_check;
    double gc_collect;

    uint   runs;
    uint   aasize;
    string label;
    string test;

    runOptions  options;
    double  relativeTime;
    double  rel_times[];


    this()
    {
        timer = new PerformanceCounter();
        relativeTime = 1.0;
    }

    void configure()
    {
        insert_times = new double[runs];
        lookup_times = new double[runs];
        clear_times = new double[runs];
    }

    void normalizeTime()
    {
        if (!options.normalizeTime)
        {
            relativeTime = 1.0;
            return;
        }
        timer.start();
        relativeTimeTask();
        timer.stop();
        relativeTime = timer.microseconds / 1_000_000.0;
        rel_times ~= relativeTime;
    }

    void overrides(ref runOptions opt)
    {
        options.loadRatio = opt.loadRatio;
    }
    void reset()
    {
        post_check = 0;
        gc_collect = 0;
        rel_times.length = 0;
    }
    void outputRun(int runix)
    {
        if (runix < 0)
            return;
        writefln("%2d: %4.2f %4.2f %4.2f %4.2f",
                 runix, rel_times[runix], insert_times[runix],
                 lookup_times[runix], clear_times[runix]);
    }

    void setClear(double val, int runix)
    {
        if (runix < 0)
            return;
        clear_times[runix] = val;
    }
    void setInsert(double val, int runix)
    {
        if (runix < 0)
            return;
        insert_times[runix] = val;
    }
    void setLookup(double val, int runix)
    {
        if (runix < 0)
            return;
        lookup_times[runix] = val;
    }
    void make_shuffle( )
    {
        rand_index = new uint[aasize];
        for (uint i = 0; i < aasize; i++)
        {
            rand_index[i] = i;
        }

        if (options.randomLookup)
        {
            foreach (i; rand_index)
            {
                uint swap_ix = uniform(0, rand_index.length - i, rndGen);
                if (swap_ix != i)
                {
                    uint v1 = rand_index[i];
                    uint v2 = rand_index[swap_ix];
                    rand_index[i] = v2;
                    rand_index[swap_ix] = v1;

                }
            }
        }

    }
    void output()
    {
        /+if (options.postCheck)
        {
            // no output, just add totals to post_check
            if (runs > 0)
            {
                real sum = 0;
                for(uint rix = 0; rix < insert_times.length; rix++)
                    sum += insert_times[rix] + lookup_times[rix] + clear_times[rix];
                post_check = sum / runs;
            }

            return;
        }+/

        if (runs > 0)
        {
            //double persec = aasize; // total insertions & lookups

            void outputStat(string label, double data[])
            {
                NormalStatistic ns;

                getStatistic(data, ns);
                writefln("%s avg %5.2f sd %5.3f (%d) ",label, ns.mean, ns.stddev, data.length);
            }

            outputStat("relative time task: ", rel_times);
            writeln(aasize," * ", runs ,": ", label, " randomized lookup = ",options.randomLookup);
            writeln("clearAA = ",options.aaClear,"  Disable GC = ", options.noGC, " No capacity = ", options.noCap);

            NormalStatistic timestat;
            getStatistic(rel_times, timestat);

            double adj_time[];
            double adj_insert[];
            double adj_lookup[];
            double adj_clear[];
            for(uint kix = 0; kix < insert_times.length; kix++)
            {
                if (fabs(rel_times[kix] - timestat.mean) < timestat.stddev)
                {
                    adj_time ~= rel_times[kix];
                    adj_insert ~= insert_times[kix];
                    adj_lookup ~= lookup_times[kix];
                    adj_clear ~= clear_times[kix];
                }
            }
            writeln("Raw times");
            outputStat("time_task:", adj_time);
            outputStat("inserts:", adj_insert);
            outputStat("lookups:", adj_lookup);
            outputStat("clears:", adj_clear);

            getStatistic(adj_time, timestat);
            for(uint kix = 0; kix < adj_time.length; kix++)
            {
                adj_insert[kix] /= timestat.mean;
                adj_lookup[kix] /= timestat.mean;
                adj_clear[kix] /= timestat.mean;
            }

            writeln("Relative to timed task");
            outputStat("inserts:", adj_insert);
            outputStat("lookups:", adj_lookup);
            outputStat("clears:", adj_clear);

            double avg = post_check/runs;
            writeln("post_check: ", avg, " s  ");

            avg = gc_collect/runs;
            writeln("gc_collect: ", avg, " s  ");

        }
    }

    void read(InputStream ins)
    {
         auto cis = CheckedInputStream(ins);
         string key;

         READ_LOOP: for(;;)
         {
             cis.readString(key);
             switch(key)
             {
             case "runs":
                cis.readUint(runs);

                break;
             case "aaSize":
                cis.readUint(aasize);
                break;
             case "order":
                cis.readArray(rand_index);
                break;
             case "clear":
                cis.readBool(options.aaClear);
                break;
             case "rand-lookup":
                cis.readBool(options.randomLookup);
                break;
             case "end":
                break READ_LOOP;
             case "test":
                cis.readString(test);
                break;
             default:
                break;
             }
         }
    }
    void write(OutputStream outs)
    {
        auto chos = CheckedOutputStream(outs);


        chos.writeString("test");
        chos.writeString(test);

        chos.writeString("runs");
        chos.writeUint(runs);

        chos.writeString("aaSize");
        chos.writeUint(aasize);

        chos.writeString("order");
        chos.writeArray(rand_index);

        chos.writeString("clear");
        chos.writeBool(options.aaClear);

        chos.writeString("rand-lookup");
        chos.writeBool(options.randomLookup);

        chos.writeString("end");
    }
}
class ClassStats : RunStats {
    WrapString[] keyset;

    void init()
    {
       test = "cs";
    }

    this(ref runOptions opt)
    {
        init();
        BufferedFile bf = new BufferedFile(opt.dataFile,FileMode.In);
        scope(exit)
            bf.close();
        read(bf);
        overrides(opt);

        configure();

    }

    this(uint N, uint probsize, ref runOptions  opt)
    {
        init();
        options = opt;
        runs = N;
        aasize = probsize;

        string[] store = getStringSet(20, aasize);
        keyset = new WrapString[store.length];
        foreach(ix, s ; store)
        {
            keyset[ix] = new WrapString(s);
        }
        size_t limit = (keyset.length < 10) ? keyset.length : 10;
        for(int k = 0; k < limit; k++)
        {
            writeln(k, " ", keyset[k]);
        }

        delete store;
        this.make_shuffle();
        configure();
        options.loadRatio = opt.loadRatio;
    }
    void read(InputStream ins)
    {
        super.read(ins);
        auto cis = CheckedInputStream(ins);
        string s;
        cis.readString(s);
        if (s == "UStrings")
        {
            size_t klen;
            cis.readUint(klen);

            keyset = new WrapString[klen];
            GC.disable();
            for(uint ki = 0; ki < klen; ki++)
            {
                auto ws = new WrapString();
                ws.read(ins);
                keyset[ki] = ws;
            }
            GC.enable();
            size_t limit = (keyset.length < 5) ? keyset.length : 5;
            for(int k = 0; k < limit; k++)
            {
                writeln(k, " ", keyset[k]);
            }
            limit = keyset.length - 5;
            if (limit < 0)
                limit = 0;
            for(int k = limit; k < keyset.length; k++)
            {
                writeln(k, " ", keyset[k]);
            }
        }
    }
    void write(OutputStream outs)
    {
        super.write(outs);
        auto chos = CheckedOutputStream(outs);
        chos.writeString("UStrings");
        chos.writeUint(keyset.length);
        foreach(ss ; keyset)
            ss.write(outs);
    }

}

class ClassUintStats : RunStats {
    WrapUint[] keyset;

    void init()
    {
       test = "cu";
    }

    this(ref runOptions opt)
    {
        init();
        BufferedFile bf = new BufferedFile(opt.dataFile,FileMode.In);
        scope(exit)
            bf.close();
        read(bf);
        overrides(opt);
        configure();
        options.loadRatio = opt.loadRatio;
    }

    this(uint N, uint probsize, ref runOptions  opt)
    {
        init();
        options = opt;
        runs = N;
        aasize = probsize;

        uint[] store = getUIntSet(aasize);
        keyset = new WrapUint[store.length];
        foreach(ix, s ; store)
        {
            keyset[ix] = new WrapUint(s);
        }
        delete store;
        this.make_shuffle();
        configure();
        options.loadRatio = opt.loadRatio;
    }
    void read(InputStream ins)
    {
        super.read(ins);
        auto cis = CheckedInputStream(ins);
        string s;
        cis.readString(s);
        if (s == "UInts")
        {
            size_t klen;
            cis.readUint(klen);

            keyset = new WrapUint[klen];
            foreach( ref ss ; keyset)
            {
                ss = new WrapUint();
                ss.read(ins);
            }
        }
    }
    void write(OutputStream outs)
    {
        super.write(outs);
        auto chos = CheckedOutputStream(outs);
        chos.writeString("UInts");
        chos.writeUint(keyset.length);
        foreach(ss ; keyset)
            ss.write(outs);
    }

}

class IntStatsStringData : IntStats {
    void init()
    {
        test = "us";
    }

    this(ref runOptions opt)
    {
        super(opt);
    }

    this(uint N, uint probsize, ref runOptions opt)
    {
        super(N, probsize, opt);
    }
}
class IntStats : RunStats {
    uint[] keyset;

    void init()
    {
        test = "uu";
    }

    this(ref runOptions opt)
    {
        init();
        BufferedFile bf = new BufferedFile(opt.dataFile,FileMode.In);
        scope(exit)
            bf.close();
        read(bf);
        overrides(opt);
        configure();
        options.loadRatio = opt.loadRatio;
    }
    this(uint N, uint probsize, ref runOptions opt)
    {
        init();
        options = opt;



        runs = N;
        aasize = probsize;


        keyset = getUIntSet(probsize);

        this.make_shuffle();
        configure();
        options.loadRatio = opt.loadRatio;

    }
    void read(InputStream ins)
    {
        super.read(ins);

        auto cis = CheckedInputStream(ins);
        string s;
        cis.readString(s);
        if (s == "uints")
        {
            cis.readArray(keyset);
        }
    }
    void write(OutputStream outs)
    {
        auto chos = CheckedOutputStream(outs);
        super.write(outs);
        chos.writeString("uints");
        chos.writeArray(keyset);
    }

}



class StringStats : RunStats {
    string[] keyset;
    void init()
    {
       test = "su";
    }

    this(ref runOptions opt)
    {
        init();
        BufferedFile bf = new BufferedFile(opt.dataFile,FileMode.In);
        scope(exit)
            bf.close();
        read(bf);
        overrides(opt);
        configure();
        options.loadRatio = opt.loadRatio;
    }

    this(uint N, uint probsize, ref runOptions  opt)
    {
        init();
        options = opt;
        runs = N;
        aasize = probsize;

        keyset = getStringSet(20, aasize);

        this.make_shuffle();
        configure();
        options.loadRatio = opt.loadRatio;
    }
    void read(InputStream ins)
    {
        super.read(ins);
        auto cis = CheckedInputStream(ins);
        string s;
        cis.readString(s);
        if (s == "strings")
        {
            cis.readArray(keyset);
        }
    }
    void write(OutputStream outs)
    {
        super.write(outs);
        auto chos = CheckedOutputStream(outs);
        chos.writeString("strings");
        chos.writeArray(keyset);
    }

}

double postEnvCheck(RunStats rr)
{
    /+
    runOptions opt;

    opt.aaClear = true;
    opt.randomLookup = true;
    opt.postCheck = true;

    IntStats ii = new IntStats(5u,250000u,opt);
    testRandom!(test_aaprlpy)(ii);
    double result = ii.post_check;
    rr.post_check += ii.post_check;
    return result;
    +/
    return 1.0;
}

bool runSelect(uint bigN,uint dictSize, string implem, string test, ref runOptions opt)
{
    RunStats rs;

    if (opt.readGen)
    {
        BufferedFile bf = new BufferedFile(opt.dataFile, FileMode.In);
        auto cis = CheckedInputStream(bf);
        string s;

        cis.readString(s);
        if (s == "test")
        {
            cis.readString(test);
            switch(test)
            {
            case "us":
                rs = new IntStatsStringData(opt);
                break;
            case "uu":
                rs = new IntStats(opt);
                break;
            case "su":
                rs = new StringStats(opt);
                break;
            case "cs":
                rs = new ClassStats(opt);
                break;
            case "cu":
                rs = new ClassUintStats(opt);
                break;
            default:
                break;
            }
        }
    }
    else {
        switch(test)
        {
        case "uu":
            rs = new IntStats(bigN,dictSize,opt);
            break;
        case "su":
            rs = new StringStats(bigN,dictSize,opt);
            break;
        case "cs":
            rs = new ClassStats(bigN,dictSize,opt);
            break;
        case "us":
            rs = new IntStatsStringData(bigN,dictSize,opt);
            break;
        case "cu":
            rs = new ClassUintStats(bigN,dictSize,opt);
            break;
        default:

            break;
        }
    }

    if (rs is null)
    {
        writeln("No test data or kind specified");
        return false;
    }
    string select = text(implem,"-",test);
    if (bigN > 0)
        rs.runs = bigN;
    writeln("n=", rs.runs, "m=", rs.aasize, "random=", rs.options.randomLookup);
    switch(select)
    {
    case "builtin-us":
        testRandomStringData!(test_builtin)(cast(IntStats) rs);
        break;
    case "builtin-uu":
        testRandom!(test_builtin)(cast(IntStats) rs);
        break;
    case "builtin-su":
        testLinear!(test_builtin)(cast(StringStats)rs);
        break;
    case "builtin-cs":
        testClass!(test_builtin)(cast(ClassStats)rs);
        break;
    case "builtin-cu":
        testClassUint!(test_builtin)(cast(ClassUintStats)rs);
        break;


    case "builtin-hm-us":
        testRandomStringData!(test_drhm)(cast(IntStats) rs);
        break;
    case "builtin-hm-uu":
        testRandom!(test_drhm)(cast(IntStats) rs);
        break;
    case "builtin-hm-su":
        testLinear!(test_drhm)(cast(StringStats)rs);
        break;
    case "builtin-hm-cs":
        testClass!(test_drhm)(cast(ClassStats)rs);
        break;
    case "builtin-hm-cu":
        testClassUint!(test_drhm)(cast(ClassUintStats)rs);
        break;

   case "hashtree-us":
        testRandomStringData!(test_aaht)(cast(IntStats) rs);
        break;
    case "hashtree-uu":
        testRandom!(test_aaht)(cast(IntStats) rs);
        break;
    case "hashtree-su":
        testLinear!(test_aaht)(cast(StringStats)rs);
        break;
    case "hashtree-cs":
        testClass!(test_aaht)(cast(ClassStats)rs);
        break;
    case "hashtree-cu":
        testClassUint!(test_aaht)(cast(ClassUintStats)rs);
        break;

   case "hashptr-us":
        testRandomStringData!(test_aahp)(cast(IntStats) rs);
        break;
    case "hashptr-uu":
        testRandom!(test_aahp)(cast(IntStats) rs);
        break;
    case "hashptr-su":
        testLinear!(test_aahp)(cast(StringStats)rs);
        break;
    case "hashptr-cs":
        testClass!(test_aahp)(cast(ClassStats)rs);
        break;
    case "hashptr-cu":
        testClassUint!(test_aahp)(cast(ClassUintStats)rs);
        break;

    case "pydict-uu":
        testRandom!(test_aapy)(cast(IntStats) rs);
        break;
    case "pydict-su":
        testLinear!(test_aapy)(cast(StringStats)rs);
        break;
    case "pydict-cs":
        testClass!(test_aapy)(cast(ClassStats) rs);
        break;
    case "pydict-cu":
        testClassUint!(test_aapy)(cast(ClassUintStats)rs);
        break;



    case "hashmap-uu":
        testRandom!(test_aahm)(cast(IntStats) rs);
        break;
    case "hashmap-su":
        testLinear!(test_aahm)(cast(StringStats)rs);
        break;
    case "hashmap-cs":
        testClass!(test_aahm)(cast(ClassStats) rs);
        break;
    case "hashmap-cu":
        testClassUint!(test_aahm)(cast(ClassUintStats)rs);
        break;


    case "parallel-rd-uu":
        testRandom!(test_aaprlrd)(cast(IntStats) rs);
        break;
    case "parallel-rd-su":
        testLinear!(test_aaprlrd)(cast(StringStats)rs);
        break;
    case "parallel-py-uu":
        testRandom!(test_aaprlpy)(cast(IntStats) rs);
        break;
    case "parallel-py-su":
        testLinear!(test_aaprlpy)(cast(StringStats)rs);
        break;


    case "hashlist-us":
        testRandomStringData!(test_aahl)(cast(IntStats) rs);
        break;
    case "hashlist-uu":
        testRandom!(test_aahl)(cast(IntStats) rs);
        break;
    case "hashlist-su":
        testLinear!(test_aahl)(cast(StringStats) rs);
        break;
    case "hashlist-cs":
        testClass!(test_aahl)(cast(ClassStats) rs);
        break;
    case "hashlist-cu":
        testClassUint!(test_aahl)(cast(ClassUintStats) rs);
        break;

    case "hashtablist-us":
        testRandomStringData!(test_aahtl)(cast(IntStats) rs);
        break;
    case "hashtablist-uu":
        testRandom!(test_aahtl)(cast(IntStats) rs);
        break;
    case "hashtablist-su":
        testLinear!(test_aahtl)(cast(StringStats) rs);
        break;
    case "hashtablist-cs":
        testClass!(test_aahtl)(cast(ClassStats) rs);
        break;
    case "hashtablist-cu":
        testClassUint!(test_aahtl)(cast(ClassUintStats) rs);
        break;
   default:
        return false;
        break;

    }
    return true;
}

string gAppName;


void showUsage()
{

    writeln("test n=<N>  m=<dict-size> -i <implementation> -t <test> -nc -r");
    writeln("implementation:  builtin, hashtree, pydict, hashmap, parallel-py, parallel-rd");
    writeln("-t test:   su (string-uint),  uu (uint-uint)");
    writeln("-r     :   random shuffle for lookup");
    writeln("-g  <file>   :   generate test data");
    writeln("-d  <file>   :   use test data");
    writeln("-l  <number>   :   load ratio");
    writeln("-nc     :   No clear");
    writeln("-ngc    :   No garbage collection during timing");
    writeln("-ncap    :   No call to capacity before insertions");
		

}




void main(char[][] args)
{

       // unittest_builtin();
	test_index();
	uint bigN = 0;
	uint dictSize = 50000;

    runOptions opt;


    int select = -1;

    if (args.length > 0)
        gAppName = args[0].idup;

    //unittest_builtin();

    auto argin = ArrayInputRange!(char[])(args[1..$]);

    /*bool nextArg(ref uint ix, ref  string argval)
    {
        if (ix < args.length)
        {
            argval = args[ix++].idup;
            return true;
        }
        return false;
    }*/


    WrapString ws1 = new WrapString("TestIdentity");
    WrapString ws2 = new WrapString("TestIdentity");

    writeln("ws1 opCmp ws2 ", ws1.opCmp(ws2) );
    writeln("ws1 is ws2 ", ws1 is ws2);
    writeln("ws1 opEquals ws2 ", ws1.opEquals(ws2));



    string implementation;
    string testkv;
    string loadRatio;
    bool   noclear = false;
    bool   generate = false;

    //uint argix = 1;

    void getDataFile()
    {
        if (argin.empty)
        {
            showUsage();
            writeln("data file name expected");
            return;
        }
        opt.dataFile = argin.front.idup;
        argin.popFront;
    }

    bool nextArg(ref string val)
    {
        if (argin.empty)
            return false;
        val = argin.front.idup;
        argin.popFront;
        return true;
    }
    string arg;
    while(nextArg(arg))
    {

        if (arg.length > 1)
        {
            string frag = arg[0..2];
            switch(frag)
            {
            case "-d":
                opt.readGen = true;
                getDataFile();
                break;
            case "-g":
                opt.generate = true;
                getDataFile();
                break;
            case "m=":
                dictSize = to!(uint)(arg[2..$]);
                break;
            case "n=":
                bigN = to!(uint)(arg[2..$]);
                break;
            case "-i":
                if (!nextArg(implementation))
                {
                    showUsage();
                    writeln("implementation name expected");
                    return;
                }
                break;
            case "-l":
                if (nextArg(loadRatio))
                {
                    opt.loadRatio = to!(double)(loadRatio);
                    if (opt.loadRatio < 0.5)
                        opt.loadRatio = 0.5;
                    else if (opt.loadRatio > 16.0)
                        opt.loadRatio = 16.0;
                }
                else {
                    showUsage();
                    writeln("load factor expected");
                    return;
                }
                break;
            case "-t":
                if (!nextArg(testkv))
                {
                    showUsage();
                    writeln("test name expected");
                    return;
                }
                break;
            case "-n":
                if (arg == "-nrandom")
                    opt.randomLookup = false;
                else if (arg == "-nc")
                    opt.aaClear = false;
                else if (arg == "-ngc")
                    opt.noGC = true;
				else if (arg == "-ncap")
					opt.noCap = true;
                else
                {
                    showUsage();
                    return;
                }
                break;

            default:
                showUsage();
                writeln("unknown option ", arg);
                return;
                break;

            }
        }

    }

    if (opt.readGen && opt.generate)
    {
        showUsage();
        writeln("Cannot both generate data and use a data file");
        return;
    }

    if (opt.generate)
    {
        BufferedFile outs = new BufferedFile(opt.dataFile,FileMode.OutNew);
        scope(exit)
            outs.close();

        switch(testkv)
        {
        case "us":
            IntStats stats = new IntStatsStringData(bigN, dictSize,opt);
            stats.write(outs);
            break;
       case "uu":
            IntStats stats = new IntStats(bigN, dictSize,opt);
            stats.write(outs);
            break;
        case "su":
             StringStats stats = new StringStats(bigN, dictSize,opt);
            stats.write(outs);
           break;
        case "cs":
            ClassStats stats = new ClassStats(bigN, dictSize,opt);
            stats.write(outs);
            break;
        case "cu":
            ClassUintStats stats = new ClassUintStats(bigN, dictSize,opt);
            stats.write(outs);
            break;
        default:
            writeln("Bad test type ", testkv);
            break;
        }
        return;
    }

    if (!runSelect(bigN, dictSize, implementation, testkv, opt))
    {
        showUsage();
        return;
    }

}

void showHashStats(uint[] lstat, uint aasize)
{
    if (lstat.length == 0)
    {
        writeln("Empty stats array");
        return;
    }

    uint sum = 0;
    uint tableSize = lstat[0];

    writefln("hash table size %d occupied %6.2f %% ratio %5.3f",tableSize, ((tableSize - lstat[1]) * 100.0)/ tableSize, cast(double)aasize/tableSize);
    real pcent = 100.0 / aasize;
    real accum = 0;

    for(uint blen = 2; blen < lstat.length; blen++)
    {
        uint ct = lstat[blen];
        if (ct > 0)
        {
            uint listct = blen-1;
            uint nodect = listct * ct;
            real dist = nodect * pcent;
            accum += dist;
            writefln("length %d %6.2f %%, %6.2f %%", listct, dist, accum);
            sum += nodect;
        }
    }
    writefln("total %d, aasize %d", sum, aasize);
}
uint rand()
{
    auto ret = rndGen.front;
    rndGen.popFront;
    return ret;
}

void runLinear(alias AA)(int ix, StringStats ss)
{

    mixin AA!(string,uint);
    string[] keyset = ss.keyset;


    allocate(aa);
    if (ss.options.loadRatio > 0.0)
        loadRatio(aa, ss.options.loadRatio);


    //writeln("Test Linear Insert.");
    auto timer = ss.timer;
     if (ss.options.noGC)
        GC.disable();
   ss.normalizeTime();
   timer.start();
   if (!ss.options.noCap)
	  capacity(aa, keyset.length);
    for (uint i=ss.aasize ; i--;)
    {
        version (RandomValue)
            aa[keyset[i]] = i;
        else
            aa[keyset[i]] = 0;
    }
    timer.stop();
    if (ss.options.noGC)
        GC.enable();
    double tinsert = timer.microseconds / 1_000_000.0;

    ss.setInsert(tinsert, ix);
    static if (is(typeof(aa.rehash)))
    {
        //writeln("Call rehash");
        aa.rehash;
    }

    static if (is(typeof(aa.loadRatio)))
    {
        if (ix == -1)
            writeln("load ratio = ", aa.loadRatio);
    }
    static if (is(typeof(aa.misses)))
    {
       if (ix == -1) {
            writeln("Misses = ", aa.misses, " rehash = ", aa.rehash_ct, " capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(aa.capacity)))
    {
       if (ix == -1) {
            writeln("capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(list_stats)))
    {
      if (ix == -1) {
             showHashStats( list_stats(aa), aa.length);
      }
    }


    //writeln("Test Linear Lookup.");
     uint[] rdex = ss.rand_index;
    if (ss.options.noGC)
        GC.disable();
    timer.start();
    for (uint i=0; i < keyset.length; i++)
    {
        uint rx = rdex[i];
        auto foo = (keyset[rx] in aa);
        if (foo is null)
            throw new TestException(text("key lookup failed ",i, " ", rx) );

    }
    timer.stop();
     if (ss.options.noGC)
        GC.enable();
   double tlookup = timer.microseconds / 1_000_000.0;

    ss.setLookup(tlookup, ix);


    double tpost;

    if(!ss.options.postCheck)
        tpost = postEnvCheck(ss);

    double tclear;
    if (ss.options.aaClear)
    {
        timer.start();
        aa_cleanup();
        timer.stop();
        tclear = timer.microseconds / 1_000_000.0;
        ss.setClear(tclear, ix);
    }
    if(!ss.options.postCheck)
    {
         ss.outputRun(ix);
    }


}

void testLinear(alias AA)(StringStats ss)
{
    ss.label = text("uint[string] for ", AA.stringof);

    if (!ss.options.postCheck)
        runGC(ss);
    ss.reset();
    if (!ss.options.postCheck)
        writeln("test ", ss.label);



    runLinear!(AA)(-1,ss);


    if (!ss.options.postCheck)
        runGC(ss);
    ss.reset();

    PerformanceCounter timer = ss.timer;

    for (size_t i = 0; i < ss.runs; i++)
    {


       runLinear!(AA)(i,ss);
       if (!ss.options.postCheck)
       {
            runGC(ss);
       }

    }
    ss.output();
}

void runRandom(alias AA)(int ix, IntStats ii)
{

    mixin AA!(uint,uint);
    auto timer = ii.timer;
    uint[] keyset = ii.keyset;


    allocate(aa);
    if (ii.options.loadRatio > 0.0)
        loadRatio(aa, ii.options.loadRatio);


    if (ii.options.noGC)
        GC.disable();
    ii.normalizeTime();
    timer.start();
    if (!ii.options.noCap)
		capacity(aa, keyset.length);
    for(uint i = 0; i < keyset.length; i++)
    {
        uint r = keyset[i];
        version (RandomValue)
            aa[r] = r;
        else
            aa[r] = 0;
    }

    timer.stop();
    if (ii.options.noGC)
        GC.enable();
    double tinsert = timer.microseconds / 1_000_000.0;

    ii.setInsert(tinsert, ix);

    static if (is(typeof(aa.rehash)))
    {
        //writeln("Call rehash");
        aa.rehash;
    }

    static if (is(typeof(aa.loadRatio)))
    {
        if (ix == -1)
            writeln("load ratio = ", aa.loadRatio);
    }

    static if (is(typeof(aa.misses)))
    {
       if (ix == -1) {
            writeln("Misses = ", aa.misses, " rehash = ", aa.rehash_ct, " capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(aa.capacity)))
    {
       if (ix == -1) {
            writeln("capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(list_stats)))
    {
        //aa.rehash;
      if (ix == -1) {
             showHashStats( list_stats(aa), aa.length);
      }
    }
    uint[] rdex = ii.rand_index;
    if (ii.options.noGC)
        GC.disable();

    timer.start();
    for(uint i = 0; i < keyset.length; i++)
    {
        uint r = rdex[i];
        r = keyset[r];
        auto val = r in aa;
        if (val is null) // show the compiler we care
            throw new TestException(text("key lookup failed for ",i, " ", r));
    }
    timer.stop();
    if (ii.options.noGC)
        GC.enable();
    double tlookup = timer.microseconds / 1_000_000.0;
    ii.setLookup(tlookup, ix);


    double tpost;
    if (!ii.options.postCheck)
        tpost = postEnvCheck(ii);

    double tclear;
    if (ii.options.aaClear)
    {
        timer.start();
        aa_cleanup();
        timer.stop();
        tclear = timer.microseconds / 1_000_000.0;
        ii.setClear(tclear, ix);

    }
    if (!ii.options.postCheck)
    {
        ii.outputRun(ix);
    }

}

void runGC(RunStats rs)
{
    PerformanceCounter timer = rs.timer;

    timer.start();
    GC.collect();
    timer.stop();

   // GCStats stats = gc_stats();

    double tgc = timer.microseconds / 1_000_000.0;
    //writeln("gc: ",tgc, " used: ", stats.usedsize, " free: ", stats.freelistsize);

    rs.gc_collect += tgc;

}
void testRandom(alias AA)(IntStats ii)
{
    double time = 0;

    if (!ii.options.postCheck)
        runGC(ii); // before the stack is setup
    ii.reset();

    ii.label = text( "uint[uint] for ", AA.stringof);
    if (!ii.options.postCheck)
        writeln("test ", ii.label);

   if (!ii.options.postCheck)
     runGC(ii);

    runRandom!(AA)(-1,ii);
    if (!ii.options.postCheck)
        runGC(ii); // before the stack is setup
    ii.reset();

    PerformanceCounter timer = ii.timer;
    for(uint i = 0; i < ii.runs; ++i)
    {

        runRandom!(AA)(i,ii);

        if (!ii.options.postCheck)
        {
            runGC(ii);
        }

    }
    ii.output();
}

void testRandomStringData(alias AA)(IntStats ii)
{
    double time = 0;
    string[] values = getStringSet(20,ii.keyset.length);

    if (!ii.options.postCheck)
        runGC(ii); // before the stack is setup
    ii.reset();

    ii.label = text( "string[uint] for ", AA.stringof);
    if (!ii.options.postCheck)
        writeln("test ", ii.label);

   if (!ii.options.postCheck)
     runGC(ii);

    runRandomStringData!(AA)(-1,ii, values);
    if (!ii.options.postCheck)
        runGC(ii); // before the stack is setup
    ii.reset();


    PerformanceCounter timer = ii.timer;
    for(uint i = 0; i < ii.runs; ++i)
    {

        runRandomStringData!(AA)(i,ii,values);

        if (!ii.options.postCheck)
        {
            runGC(ii);
        }

    }
    ii.output();
}

void runClass(alias AA)(int ix, ClassStats ii)
{

    mixin AA!(WrapString,uint);
    auto timer = ii.timer;
    WrapString[] keyset = ii.keyset;

    allocate(aa);
     if (ii.options.loadRatio > 0.0)
        loadRatio(aa, ii.options.loadRatio);



    if (ii.options.noGC)
        GC.disable();
    ii.normalizeTime();
    timer.start();
    if (!ii.options.noCap)
		capacity(aa, keyset.length);
    for(uint i = 0; i < keyset.length; i++)
    {
        WrapString r = keyset[i];
        version (RandomValue)
            aa[r] = i;
        else
            aa[r] = 0;
    }

    timer.stop();

    double tinsert = timer.microseconds / 1_000_000.0;

    ii.setInsert(tinsert, ix);

    static if (is(typeof(aa.rehash)))
    {
        //writeln("Call rehash");
        aa.rehash;
    }

    if (ii.options.noGC)
        GC.enable();
    static if (is(typeof(aa.loadRatio)))
    {
        if (ix == -1)
            writeln("load ratio = ", aa.loadRatio);
    }

    static if (is(typeof(aa.misses)))
    {
       if (ix == -1) {
            writeln("Misses = ", aa.misses, " rehash = ", aa.rehash_ct, " capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(aa.capacity)))
    {
       if (ix == -1) {
            writeln("capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(list_stats)))
    {

      if (ix == -1) {

             showHashStats( list_stats(aa), aa.length);
      }
    }
    uint[] rdex = ii.rand_index;
    if (ii.options.noGC)
        GC.disable();

    timer.start();
    for(uint i = 0; i < keyset.length; i++)
    {
        uint r = rdex[i];
        WrapString cs = keyset[r];
        auto val = cs in aa;
        if (val is null) // show the compiler we care
            throw new TestException(text("key lookup failed ", i, " ", cs.toString()));
    }
    timer.stop();





    if (ii.options.noGC)
        GC.enable();

    double tlookup = timer.microseconds / 1_000_000.0;
    ii.setLookup(tlookup, ix);


    double tpost;
    if (!ii.options.postCheck)
        tpost = postEnvCheck(ii);

    double tclear;
    if (ii.options.aaClear)
    {
        timer.start();
        aa_cleanup();
        timer.stop();
        tclear = timer.microseconds / 1_000_000.0;
        ii.setClear(tclear, ix);

    }
    if (!ii.options.postCheck)
    {
        ii.outputRun(ix);
    }

}

void testClass(alias AA)(ClassStats ii)
{
    double time = 0;

    if (!ii.options.postCheck)
        runGC(ii); // before the stack is setup
    ii.reset();

    ii.label = text( "uint[UString] for ", AA.stringof);
    if (!ii.options.postCheck)
        writeln("test ", ii.label);

   if (!ii.options.postCheck)
     runGC(ii);

    runClass!(AA)(-1,ii);
    if (!ii.options.postCheck)
        runGC(ii); // before the stack is setup
    ii.reset();

    PerformanceCounter timer = ii.timer;
    for(uint i = 0; i < ii.runs; ++i)
    {

        runClass!(AA)(i,ii);

        if (!ii.options.postCheck)
        {
            runGC(ii);
        }

    }
    ii.output();
}

void runClassUint(alias AA)(int ix, ClassUintStats ii)
{

    mixin AA!(WrapUint,uint);
    auto timer = ii.timer;
    WrapUint[] keyset = ii.keyset;

    allocate(aa);
     if (ii.options.loadRatio > 0.0)
        loadRatio(aa, ii.options.loadRatio);


    if (ii.options.noGC)
        GC.disable();

    ii.normalizeTime();
    timer.start();
   	if (!ii.options.noCap)
		capacity(aa, keyset.length);
    for(uint i = 0; i < keyset.length; i++)
    {
        WrapUint r = keyset[i];
        version (RandomValue)
            aa[r] = i;
        else
            aa[r] = 0;
    }

    timer.stop();
   if (ii.options.noGC)
        GC.enable();

    double tinsert = timer.microseconds / 1_000_000.0;

    ii.setInsert(tinsert, ix);

    static if (is(typeof(aa.rehash)))
    {
        //writeln("Call rehash");
        aa.rehash;
    }
    static if (is(typeof(aa.loadRatio)))
    {
        if (ix == -1)
            writeln("load ratio = ", aa.loadRatio);
    }
    static if (is(typeof(aa.misses)))
    {
       if (ix == -1) {
            writeln("Misses = ", aa.misses, " rehash = ", aa.rehash_ct, " capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(aa.capacity)))
    {
       if (ix == -1) {
            writeln("capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(list_stats)))
    {
      if (ix == -1) {

             showHashStats( list_stats(aa), aa.length);
      }
    }
    uint[] rdex = ii.rand_index;

    if (ii.options.noGC)
        GC.disable();

    timer.start();
    for(uint i = 0; i < keyset.length; i++)
    {
        uint r = rdex[i];
        WrapUint cs = keyset[r];
        auto val = cs in aa;
        if (val is null) // show the compiler we care
            throw new TestException("key lookup failed");
    }
    timer.stop();

    if (ii.options.noGC)
        GC.enable();

    double tlookup = timer.microseconds / 1_000_000.0;
    ii.setLookup(tlookup, ix);


    double tpost;
    if (!ii.options.postCheck)
        tpost = postEnvCheck(ii);

    double tclear;
    if (ii.options.aaClear)
    {
        timer.start();
        aa_cleanup();
        timer.stop();
        tclear = timer.microseconds / 1_000_000.0;
        ii.setClear(tclear, ix);

    }
    if (!ii.options.postCheck)
    {
        ii.outputRun(ix);
    }

}

void testClassUint(alias AA)(ClassUintStats ii)
{
    double time = 0;

    if (!ii.options.postCheck)
        runGC(ii); // before the stack is setup
    ii.reset();

    ii.label = text( "uint[ClassUint] for ", AA.stringof);
    if (!ii.options.postCheck)
        writeln("test ", ii.label);

   if (!ii.options.postCheck)
     runGC(ii);

    runClassUint!(AA)(-1,ii);
    if (!ii.options.postCheck)
        runGC(ii); // before the stack is setup
    ii.reset();

    PerformanceCounter timer = ii.timer;
    for(uint i = 0; i < ii.runs; ++i)
    {

        runClassUint!(AA)(i,ii);

        if (!ii.options.postCheck)
        {
            runGC(ii);
        }

    }
    ii.output();
}



void runRandomStringData(alias AA)(int ix, IntStats ii, string[] values)
{

    mixin AA!(uint,string);
    auto timer = ii.timer;
    uint[] keyset = ii.keyset;

    allocate(aa);
    loadRatio(aa, ii.options.loadRatio);



    if (ii.options.noGC)
        GC.disable();
    ii.normalizeTime();
    timer.start();
    if (!ii.options.noCap)
		capacity(aa, keyset.length);
    for(uint i = 0; i < keyset.length; i++)
    {
        uint r = keyset[i];
        aa[r] = values[i];
    }

    timer.stop();
    if (ii.options.noGC)
        GC.enable();
    double tinsert = timer.microseconds / 1_000_000.0;

    ii.setInsert(tinsert, ix);
    static if (is(typeof(aa.rehash)))
    {
        //writeln("Call rehash");
        aa.rehash;
    }

    static if (is(typeof(aa.loadRatio)))
    {
        if (ix == -1)
            writeln("load ratio = ", aa.loadRatio);
    }

    static if (is(typeof(aa.misses)))
    {
       if (ix == -1) {
            writeln("Misses = ", aa.misses, " rehash = ", aa.rehash_ct, " capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
        }
    }
    else static if (is(typeof(aa.capacity)))
    {
       if (ix == -1) {
            writeln("capacity = ", aa.capacity);
            showHashStats( aa.list_stats(), aa.length);
       }
    }
    else static if (is(typeof(list_stats)))
    {
      if (ix == -1) {
             showHashStats( list_stats(aa), aa.length);
      }
    }
    uint[] rdex = ii.rand_index;
    if (ii.options.noGC)
        GC.disable();

    timer.start();
    for(uint i = 0; i < keyset.length; i++)
    {
        uint r = rdex[i];
        r = keyset[r];
        auto val = r in aa;
        if (val is null) // show the compiler we care
            throw new TestException("key lookup failed");
    }
    timer.stop();
    if (ii.options.noGC)
        GC.enable();
    double tlookup = timer.microseconds / 1_000_000.0;
    ii.setLookup(tlookup, ix);


    double tpost;
    if (!ii.options.postCheck)
        tpost = postEnvCheck(ii);

    double tclear;
    if (ii.options.aaClear)
    {
        timer.start();

        aa.clear;
        timer.stop();
        tclear = timer.microseconds / 1_000_000.0;
        ii.setClear(tclear, ix);

    }
    if (!ii.options.postCheck)
    {
        ii.outputRun(ix);
    }

}


