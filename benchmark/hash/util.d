module hash.util;

import std.random;
import std.exception;


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
    Tdelegate = 'D',

    Tconst    = 'x',
    Tinvariant = 'y',
}

private immutable MangleTI hashTypeMangles[] =
[
    MangleTI.Tbool,
	MangleTI.Tbyte, MangleTI.Tubyte, MangleTI.Tshort, MangleTI.Tushort,
	MangleTI.Tint, MangleTI.Tuint, MangleTI.Tifloat,
    MangleTI.Tchar, MangleTI.Twchar, MangleTI.Tdchar,
	MangleTI.Tpointer
];

extern (D) alias int delegate(void *) dg_t;
extern (D) alias int delegate(void *, void *) dg2_t;

/** If TypeInfo indicates the type will fit into sizeof hash_t,
    and its not a class, struct or interface. List of scaler types only.
    
*/
bool IsOwnHashType(TypeInfo ifti)
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
/// randomizes lower bits
hash_t overhash(hash_t h)
{
    h ^= (h >>> 20) ^ (h >>> 12);
    return h ^ (h >>> 7) ^ (h >>> 4);
}

/**
    From "Hash Functions" , Paul Hsieh
    http://www.azillionmonkeys.com/qed/hash.html
*/

Random numGenerator;

uint superHash (const(char)[] s)
{
	const(char)* data = s.ptr;
	uint len = s.length;
	
    uint hash = len;
    uint tmp;
    int  rem;


	
    if (len <= 0 || data is null) return 0;

    rem = len & 3;  // length % 4
    len >>= 2;	    // length / 4

    /* Main loop */
    auto d8 = cast(const(ubyte)*)data;

    ushort b16 = void;
    for (;len > 0; len--) {
 		
        b16 = cast(ushort)((d8[1] << 8) + d8[0]);
        hash  += b16;

        d8 += 2;
		
        b16 = cast(ushort)((d8[1] << 8) + d8[0]);
        tmp = (b16 << 11) ^ hash;
        hash   = (hash << 16) ^ tmp;
        hash  += hash >> 11;
        d8 += 2;
    }

    /* Handle end cases */
    switch (rem) {
        case 3:
                b16 = cast(ushort)((d8[1] << 8) + d8[0]);
                hash += b16;
                hash ^= hash << 16;
                hash ^= d8[2] << 18;
                hash += hash >> 11;
                break;
        case 2:
                b16 = cast(ushort)((d8[1] << 8) + d8[0]);
                hash += b16;
                hash ^= hash << 11;
                hash += hash >> 17;
                break;
        case 1:
                hash += *d8;
                hash ^= hash << 10;
                hash += hash >> 1;
        default:
                break;

    }

    /* Force "avalanching" of final 127 bits */
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}

uint superHash (const(wchar)[] s)
{
	const(wchar)* data = s.ptr;
	uint len = s.length;
	
    uint hash = len;
    uint tmp;
    int  rem;


	
    if (len <= 0 || data is null) return 0;

    rem = len & 1; // length % 2
    len >>= 1; // length / 2

    /* Main loop */
    const(ubyte)* d8 = void;
    ushort b16 = void;
    for (;len > 0; len--) {
        hash  += *data++;
        tmp = (*data++ << 11) ^ hash;
        hash   = (hash << 16) ^ tmp;
        hash  += hash >> 11;
    }

    /* Handle end cases */
    switch (rem) {
        case 1:
                hash += *data;
                hash ^= hash << 11;
                hash += hash >> 17;
                break;
        default:
                break;

    }

    /* Force "avalanching" of final 127 bits ?? */
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}

template binary_search( T )
{
	/** return index of match or next greater value */
	int upperBound(const(T)[] data, T value)
	{
		int a = 0;
		int b = data.length;
		while (a < b)
		{
			invariant mid = a + (b-a)/2;
			invariant test = data[mid];
			if (test < value)
			{
				a = mid+1;
			}
			else if (value < test)
			{
				b = mid;	
			}
			else {
				// exact match equals upperBounde
				return mid;
			}
		}
		// not found 
		return a;
	}
}

// prime values were taken from gnu\hash.c, plus a few smaller values added
private immutable
size_t[] prime_list = [
    7UL, 13UL, 29UL,
 	53UL, 97UL, 193UL, 389UL,
 	769UL, 1_543UL, 3_079UL, 6_151UL,
 	12_289UL, 24_593UL, 49_157UL, 98_317UL,
 	196_613UL, 393_241UL, 786_433UL, 1_572_869UL,
 	3_145_739UL, 6_291_469UL, 12_582_917UL, 25_165_843UL,
 	50_331_653UL, 100_663_319UL, 201_326_611UL, 402_653_189UL,
 	805_306_457UL, 1_610_612_741UL,3_221_225_473UL, 4_294_967_291UL
];



size_t getNextPrime(size_t atLeast)
{
	int ix = binary_search!uint.upperBound(prime_list, atLeast);
	if (ix >= 0)
		return prime_list[ix];
	else
		throw new AAError("getNextPrime failed");
}

size_t getNextPower2(size_t i)
{
	size_t n = 1;
	while(n < i) {
		n <<= 1;
		if(n == 0)
			return 1 << 31; // TODO: 64 bits?
	}
	return n;
}

class AAError : Exception {
    this(string msg)
    {
        super(msg);
    }
}
class AAInitError : AAError {
	this()
	{
		super("AA not initialised");
	}
}
class AAKeyError : AAError {
	this(string msg)
	{
		super(msg);
	}
}

/**
    random string generation
*/
string randString(real expectedLength, ) {
	char[] ret;
	real cutoff = 1.0L / expectedLength;
	real randNum = uniform(0.0L, 1.0L,numGenerator);
	ret ~=  uniform!"[]"(0x20, 0x7E,numGenerator); // at least one character
	while(randNum > cutoff) {
		ret ~= uniform!"[]"(0x20, 0x7E, numGenerator);
		randNum = uniform(0.0L, 1.0L,numGenerator);
	}

	return assumeUnique(ret);
}

/**
    random string set generation
*/
string[] getStringSet(uint smax, uint ntotal)
{
    bool[string]   aa;

    scope(exit)
        aa.clear();

	while(aa.length < ntotal)
	{
	    aa[randString(smax)] = true;
	}
	return aa.keys;
}
/**
    random uint set generation
*/
uint[] getUIntSet(uint ntotal)
{
    const uint ttotal = 512 * 1024 * 1024; // Problem size s
    const uint tsmall = ttotal / 5;    // i.e. the target space

    const uint nsmall = ntotal / 4;
    const uint nlarge = ntotal  - nsmall;


    bool[uint]   aa;

    scope(exit)
        aa.clear();

	while(aa.length < ntotal)
	{
	    auto r = uniform(0U, uint.max, numGenerator) << 4;
	    aa[r ] = true;
	}

    return aa.keys;
}
