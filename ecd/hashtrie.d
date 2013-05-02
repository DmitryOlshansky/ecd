module ecd.hashtrie;

import core.exception;
import core.bitop;
import core.stdc.stdlib;
import core.stdc.string;

import std.stdio;
import std.conv;

import std.typecons, std.traits;

interface Allocator{
    void* alloc(size_t size);
    void dealloc(void*block);
}

T* make(T, Args...)(Allocator alloc, Args args)
{
    void* mem = alloc.alloc(T.sizeof);
    scope(failure) alloc.dealloc(mem);
    return emplace(cast(T*)mem, args);
}

T* emptyArray(T)(Allocator alloc, size_t len)
{
    void* mem = alloc.alloc(T.sizeof*len);
    memset(mem, 0, len*T.sizeof);
    return cast(T*)mem;
}

class GcHeap : Allocator{
    void* alloc(size_t size){
        return (new void[size]).ptr;
    }

    void dealloc(void* block){
    }
}

class Mallocator : Allocator{
    void* alloc(size_t size){
        return malloc(size);
    }

    void dealloc(void* block){
        return free(block);
    }
}

public struct HashTrie(K, V){
private:
    enum  STEP = 4, TSIZE = 1<<STEP, MASK = TSIZE-1;

    struct Entry{
        K key;
        V value;
        Entry* next; //collision chain (full-hash collision)
        this(K k, V v, Entry* n)
        {
            key = k;
            value = v;
            next = n;
        }
    }  

    struct Node{
        Node* subTable;
        //union{
            size_t shift; //amount to shift before lookup
            Entry* head; //value (list in case of collision)
        //}
        union{
            size_t hash; 
            size_t counter;
        }        
    }

    Node* root_;
    Allocator alloc_;
    
    public this(Allocator alloc)
    {
        alloc_ = alloc;
        root_ = make!Node(alloc_);
    }

    public void insert(K key, V value){
        size_t h = getHash(key);
        //TODO: insert doesn't need full path - last parent is enough
        auto leafParent = lookupLeafAndParent(h, root_);        
        Node* n = leafParent[0];
        Node* parent = leafParent[1];
        if(n.head == null){
            //great - empty leaf node
            n.head = make!Entry(alloc_, key, value, null);
            n.hash = h;
            if(parent != null) //very likely
                parent.counter++; //notify our parent - +1 in sub-table
        }
        else {
            //not empty leaf            
            if(n.hash == h){ //highly unlikely
                //not empty leaf node, hash is the same -> full hash collision
                //insert new item into the collision list at head
                Entry* p = make!Entry(alloc_, key, value, n.head);
                n.head = p;
                //counter in parent stays the same
            }
            else {
                size_t diff = n.hash ^ h;                
                //yet hash is different
                //turn the leaf into a node with subTable
                int shift = bsf(diff); //1st non zero bit in diff
                //round down the shift amount to the multiple of STEP
                shift -= shift % STEP;
               // writeln("SHIFT:", shift);
                n.shift = shift;
                n.subTable = emptyArray!Node(alloc_, TSIZE);
                //move the curent leaf value into the new slot sub-table
                size_t offs = (n.hash>>shift) & MASK;
               // writeln("OFFS:", offs);                
                n.subTable[offs].head = n.head;
                n.subTable[offs].hash = n.hash;
                //and the value to insert
                offs = (h>>shift) & MASK;
                //writefln("Sub-table slot taken %d for hash=%d", offs, h);
                n.subTable[offs].head = make!Entry(alloc_, key, value, null);
                n.subTable[offs].hash = h;                
                n.counter = 2; //rewrite hash with counter
                n.head = null;
            }
        }
    }    

    /// standard AA primitives
    public V* opBinaryRight(string op:"in")(K key)
    {
        Entry* entry = lookupEntry(key);
        return entry ? &entry.value  : null;
    }

    /// ditto
    public ref V opIndex(K key)
    {
        Entry* entry = lookupEntry(key);
        if(!entry)
            throw new RangeError("HashTrie - no such key");
        return entry.value;
    }
    /// ditto
    public void opIndexAssign(V value, K key)
    {
        insert(key, value);
    }

    public void remove(K key)
    {
        Node*[size_t.sizeof*8/STEP + 1] path;
        size_t h = getHash(key);
        //TODO: need more info, i.e. true full path with slots
        size_t len = lookupLeafAndPath(h, root_, path.ptr);
        Node* n = path[len-1];        
        if(n.hash != h) // the values in this slot have different hash
            return;        
        Entry* p = n.head;
        for(;;){
            if(p == null)
                return;
            if(p.key == key){
                break;
            }
            p = p.next;
        }
        //p is a node somewhere in the collision chain
        //TODO: deal with the case of p != n.head
        freeChain(n.head);
        n.head = null;
        len--;
        while(len != 0){ //99.9% of cases, otherwise the root is leaf node
            Node* parent = path[len-1];
            parent.counter--;
            // anti flip-flop, only kill node if count == 0
            // don't try to convert back to leaf
            if(parent.counter != 0)
                break;
            //unlink sub-table - turn into an empty leaf
            alloc_.dealloc(parent.subTable);
            parent.subTable = null; 
            assert(parent.head == null);
            len--;
        }
    }

    Entry* lookupEntry(K key)
    {
        size_t h = getHash(key);
        Node* p = lookupLeaf(h, root_);
        if(!p.head) //nothing in the slot where it should have been
            return null;
        if(p.hash != h) //this leaf can as well have different hash
            return null;
        Entry* entry = p.head; //walk the collision chain - very rare event
        for(;;){
            if(entry.key == key)
                return entry;
            entry = entry.next;     
            if(entry == null)
                return null;
        }
    }    

    debug void printStat()
    {
        size_t emptyLeaf, leaf, subTable;
        depthFirstInOrder(root_, (Node* n){
            if(n.subTable)
                subTable++;
            else if(n.head)
                leaf++;
            else
                emptyLeaf++;
        });
        writefln("Tables = %d, Leafs = %d, Empty leaf = %d", 
            subTable, leaf, emptyLeaf);

        writefln("Bloat factor = %g; (nodes count per key-pair)", 
            leaf ? (subTable+emptyLeaf+leaf)/cast(double)leaf : double.infinity);
    }

    void depthFirstInOrder(Node* n, scope void delegate(Node*) functor)
    {
        functor(n);
        if(n.subTable){
            foreach(ref s; n.subTable[0..TSIZE])
                depthFirstInOrder(&s, functor);
        }
    }
    
    void depthFirstPostOrder(Node* n, scope void delegate(Node*) functor)
    {        
        if(n.subTable){
            foreach(ref s; n.subTable[0..TSIZE])
                depthFirstPostOrder(&s, functor);
        }
        functor(n);
    }

    public ~this()
    {
        depthFirstPostOrder(root_,(Node* n){
            if(n.subTable)
                alloc_.dealloc(n.subTable);
            //deallocate linked-list of values
            else{
                freeChain(n.head);
            }
        });
        alloc_.dealloc(root_);
    }

    debug void print()
    {
        return printLayer(root_);
    }
    
    debug static void printLayer(Node* node)
    {
        import std.stdio;
        Node*[] layer = [node];
        Node*[] nextLayer;
        do{
            foreach(n; layer){
                if(n.subTable == null){
                    //leaf
                    if(n.head) //non-empty
                        writef("Leaf[h=%x] ", n.hash);
                    else
                        write("Leaf[*] ");            
                }
                else{
                    writef("Tab[cnt=%d] ", n.counter);  
                    foreach(ref v; n.subTable[0..TSIZE])
                        nextLayer ~= &v;
                }
            }
            writeln();
            layer = nextLayer;
            nextLayer = null;
        }while(layer.length);
        writeln();
    }
    
    void freeChain(Entry* chain)
    {
        Entry* e = chain;
        while(e){
            Entry* eNext = e.next;
            alloc_.dealloc(e);
            e = eNext;
        }
    }

    // get to the leaf node for the given hash
    static Node* lookupLeaf(size_t hash, Node* node)
    {
        for(;;){
            if(!node.subTable) //nowhere to go - leaf node
                return node;
            size_t off = (hash >> node.shift) & MASK;
            node = node.subTable+off;
        }
    }

    // get the full path and return its size
    static Tuple!(Node*, Node*) lookupLeafAndParent(size_t hash, Node* node)
    {
        size_t idx=0;
        Node* parent = null;
        while(node.subTable){
            parent = node;
            size_t off = (hash >> node.shift) & MASK;            
            node = node.subTable+off;
        }
        return tuple(node, parent);
    }

    // get the full path and return its size
    static size_t  lookupLeafAndPath(size_t hash, Node* node, Node** table)
    {
        size_t idx=0;
        for(;;){
            table[idx++] = node;
            if(!node.subTable) //nowhere to go - leaf node
                return idx;
            size_t off = (hash >> node.shift) & MASK;            
            node = node.subTable+off;
        }
    }

    static size_t getHash(K key)
    {
        return typeid(K).getHash(&key);
    }
}

public auto createHashTrie(K, V)()
{
    return HashTrie!(K, V)(new Mallocator());
}

unittest
{        
    foreach(times; 0..1800){
        auto trie = HashTrie!(int, int)(new Mallocator());
        for(int i=1; i<=900; i++){
            trie.insert(i, 10*i);
        }
        if(times == 0){
            debug trie.printStat();            
            debug trie.print();
        }
        for(int i=0; i<10000;i++)
        {
            assert(cast(bool)(i in trie) == (i >=1 && i<=900));
        }
        for(int i=0; i<10000;i++)
        {
            if(i % 7 > 2)
                trie.remove(i);
        }
        if(times == 0){
            debug trie.printStat();
            debug trie.print();
        }
        for(int i=0; i<10000;i++)
        {
            int* pi = i in trie;
            assert((pi != null) == (i % 7 <= 2 && i >=1 && i<=900));
            assert(!pi || *pi == i*10);
        }
        //wipe it and repeat the test
        for(int i=1; i<=900; i++){
            trie.remove(i);
        }
        if(times == 0){
            debug trie.printStat();
            debug trie.print();

        }
    }
}
