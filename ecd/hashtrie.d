module ecd.hashtrie;

import core.exception;
import core.bitop;

import std.stdio;

import std.typecons, std.traits;

interface Allocator{
    void* alloc(int size);
    void dealloc(void*block);
}

class BlockAllocator(size_t block){
    void* alloc(int size){
        return null;
    }

    void dealloc(void* block){

    }
}

public struct HashTrie(K, V){
private:
    enum  STEP = 2, TSIZE = 1<<STEP, MASK = TSIZE-1;

    struct Entry{
        K key;
        V value;
        Entry* next; //collision chain (full-hash collision)
    }  

    struct Node{
        Node* subTable;
        union{
            size_t shift; //amount to shift before lookup
            Entry* head; //value (list in case of collision)
        }
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
        root_ = new Node;
    }

    public void insert(K key, V value){
        size_t h = getHash(key);
        //TODO: insert doesn't need full path - last parent is enough
        auto leafParent = lookupLeafAndParent(h, root_);        
        Node* n = leafParent[0];
        Node* parent = leafParent[1];
        if(n.head == null){
            //great - empty leaf node
            n.head = new Entry(key, value, null);
            n.hash = h;
            if(parent != null) //very likely
                parent.counter++; //notify our parent -+1 in sub-table
        }
        else {
            //not empty leaf            
            if(n.hash == h){ //highly unlikely
                //not empty leaf node, hash is the same -> full hash collision
                //insert new item into the collision list at head
                Entry* p = new Entry(key, value, n.head);
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
                auto tab = new Node[TSIZE];
                n.subTable = tab.ptr;
                //move the curent leaf value into the new slot sub-table
                size_t offs = (n.hash>>shift) & MASK;
               // writeln("OFFS:", offs);                
                n.subTable[offs].head = n.head;
                n.subTable[offs].hash = n.hash;
                //and the value to insert
                offs = (h>>shift) & MASK;
                //writefln("Sub-table slot taken %d for hash=%d", offs, h);
                n.subTable[offs].head = new Entry(key, value, null);
                n.subTable[offs].hash = h;                
                n.counter = 2; //rewrite hash with counter
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

        n.head = null;
        len--;
        while(len != 0){ //99.9% of cases, otherwise the root is leaf node
            Node* parent = path[len-1];
            parent.counter--;
            //anti flip-flop, only kill node if count == 0
            // don't try to convert back to leaf
            if(parent.counter != 0)
                break;
            parent.subTable = null; //unlink sub-table - turn into an empty leaf
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
        depthFirst(root_, (Node* n){
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

    debug void depthFirst(Node* n, scope void delegate(Node*) functor)
    {
        functor(n);
        if(n.subTable){
            foreach(ref s; n.subTable[0..TSIZE])
                depthFirst(&s, functor);
        }
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

unittest
{    
    auto trie = HashTrie!(int, int)(null);
    foreach(times; 0..25){
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