module test;


import core.memory;
import std.datetime;
import std.conv;
import std.exception;
import std.random;
import std.stdio;
import pydict.pyDictD2;
import randAA.RandAA;
import ecd.hashtrie;

/*
Perform some benchmarks on the given AA implementations.

dmd test.d pydict/pyDictD2.d randAA/RandAA.d link/trie.d -O -release
*/



class BuiltIn(KEY, VALUE)
{
    VALUE[KEY] aa_;
	VALUE opIndex(ref VALUE v, KEY k)
	{
		return aa_[k];
	}	
	void opIndexAssign(ref VALUE v, KEY k)
	{
		aa_[k] = v;
	}
	
	VALUE* opIn_r(KEY k)
	{
		return (k in aa_);
	}
}

string randString(real expectedLength) {
	char[] ret;
	real cutoff = 1.0L / expectedLength;
	real randNum = uniform(0.0L, 1.0L);
	ret ~=  uniform!"[]"('a', 'z'); // at least one character
	while(randNum > cutoff) {
		ret ~= uniform!"[]"('a', 'z');
		randNum = uniform(0.0L, 1.0L);
	}
	
	return assumeUnique(ret);
}
	
string[] getStringSet(uint smax, uint ntotal)
{
	string[] result = new string[ntotal];
	
	for(uint ix = 0; ix < ntotal; ix++)
		result[ix] = randString(smax);
	return result;
}



void main(char[][] args)
{
    GC.disable();
	uint bigN = 15;
	uint dictSize = 250_000;
	 uint[uint] aatest;

	writeln("AA size = ", aatest.sizeof);
	if (args.length > 1)
	{
		bigN = to!(uint)(args[1]);
	}
	else {
		writeln("test <N>  <dict-size> ; number of runs, dictionary size, defaults are ", bigN, " " , dictSize);
	}
    //testLinearBuildIn();
    //testLinear!(RandAA)();
    //testLinear!(PyDict)();

  //  testRandom!(PyDict)(bigN, dictSize); //bogus
    testRandom!(RandAA)(bigN, dictSize);
	testRandom!(BuiltIn)(bigN, dictSize);
    testRandom!(HashTrie)(bigN, dictSize);	
    /*
	writeln("Making string keys");
	string[] keys = getStringSet(20, dictSize);
	writeln("String key insertion and lookups");
	testLinear!(PyDict)(bigN, dictSize, keys);
    testLinear!(RandAA)(bigN, dictSize, keys);
	testLinear!(BuiltIn)(bigN, dictSize, keys);    
    */
}


Xorshift128 xorRng;

static this()
{
    xorRng = Xorshift128(unpredictableSeed);
}

uint rand()
{
    auto ret = xorRng.front;
    xorRng.popFront;
    return ret;
}

void testLinear(alias AA)(uint M, uint N, string[] strings)
{
    writeln(AA.stringof ~ ":");
   

    double lookup_time = 0;
    double insert_time = 0;

    auto timer = new StopWatch();

    // Returns a random string composed of the letters [a-z] with geometrically
    // distributed length.

    
    //for string testing


    void run(uint ix)
    {
        auto aa = new AA!(string, uint)();
        
        //writeln("Test Linear Insert.");
        timer.reset();
        timer.start();
        for (uint i=N ; i--;)
        {
            aa[strings[i]] = i;
        }
        timer.stop();
		double ti = timer.peek().msecs/1000.0;
        insert_time += ti;
		
        //writeln("Test Linear Lookup.");
        timer.reset();
        timer.start();
        for (uint i=N; i--;)
        {
            auto foo = (strings[i] in aa);
        }
        timer.stop();
		double tt = timer.peek().msecs / 1000.0;
        lookup_time += tt;
		writeln(ix,": ", ti, "  ", tt );
		delete aa;
		aa = null;
    }

    for (size_t i = 0; i < M; i++)
    {
        run(i);
    }

    printf("%u x %u iterations\n", M, N);
    printf("inserts:  %u/s (%fs)\n", cast(uint) (M * N / insert_time), (insert_time / M));
    printf("lookups: %u/s (%fs)\n", cast(uint) (M * N / lookup_time), (lookup_time / M));
}


void testRandom(alias AA)(uint N, uint ntotal)
{
    const uint ttotal = 512 * 1024 * 1024; // Problem size s
    const uint tsmall = ttotal / 5;    // i.e. the target space
    const uint nsmall = ntotal / 4;
    const uint nlarge = ntotal - nsmall;

    double time = 0;
    
    auto timer = new StopWatch();
    writeln(N, " runs for ", AA.stringof);
	
    void run(uint ix)
    {
		static if(is(AA!(uint,uint) == HashTrie!(uint, uint)))
        {
            auto aa = AA!(uint, uint)(null);
        }
        else static if(is(AA!(uint, uint) == BuiltIn!(uint, uint)))
        {
            uint[uint] aa; //build-in
        }
        else
            auto aa = new AA!(uint, uint)(); 
        timer.reset();         
        timer.start();
        for(uint i = 1; i <= nlarge; i++)
        {
            auto r = rand() << 4;
            aa[r % tsmall] = i;
        }

        for(uint i = 1; i <= nsmall; i++)
        {
            auto r = rand() << 4;
            aa[r % ttotal] = i;
        }
        timer.stop();
              
		double tt = timer.peek().msecs / 1000.0;
		writeln(ix, ":  ", tt);
        time += tt;
    }

    for(uint i = 0; i < N; ++i)
    {
        run(i);
    }

    writeln("average time of ", N, " runs: ", time / N, "s");
}
