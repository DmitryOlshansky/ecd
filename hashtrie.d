module hashtrie;

import core.exception;
import core.bitop;

import std.stdio;

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
    enum  STEP = 4, TSIZE = 1<<STEP, MASK = TSIZE-1;

    struct Pair{
        K key;
        V value;
        Pair* next; //collision chain (full-hash collision)
    }  

    struct Node{
        Pair* value; //value (list in case of collision)
        union{
            size_t hash; 
            size_t counter;
        }
        size_t shift; //amount to shift before lookup        
        Node* subTable;
    }

    Node* root_;
    Allocator alloc_;
    
    public this(Allocator alloc)
    {
        alloc_ = alloc;
        root_ = new Node;
    }

    public void insert(K key, V value){
        size_t h = typeid(K).getHash(&key);
        //TODO: insert doesn't need full path - last parent is enough
        Node*[size_t.sizeof*8/STEP+1] path;
        size_t len = lookupLeafAndPath(h, root_, path.ptr);
        Node* n = path[len-1];
        writeln("HASH: ", h);
        if(n.value == null){
            //great - empty leaf node
            n.value = new Pair(key, value, null);
            n.hash = h;
            if(len > 1) //unlikely
                path[len-2].counter++; //notify our parent - +1 in sub-table
        }
        else {
            //not empty leaf            
            if(n.hash == h){ //unlikely
                //not empty leaf node, hash is the same -> full hash collision
                //insert new item into the collision list at head
                Pair* p = new Pair(key, value, n.value);
                n.value = p;
                //counter in parent stays the same
            }
            else {
                size_t diff = n.hash ^ h;                
                //yet hash is different
                //turn the leaf into a node with subTable
                int shift = bsf(diff); //1st non zero bit in diff
                //round down the shift amount to the multiple of STEP
                shift &= ~(STEP-1);
                writeln("SHIFT:", shift);
                n.shift = shift;
                auto tab = new Node[TSIZE];
                n.subTable = tab.ptr;
                //move the curent leaf value into the new slot sub-table
                size_t offs = (n.hash>>shift) & MASK;
                writeln("OFFS:", offs);                
                n.subTable[offs].value = n.value;
                n.subTable[offs].hash = n.hash;
                //and the value to insert
                offs = (h>>shift) & MASK;
                writefln("Sub-table slot taken %d for hash=%d", offs, h);
                n.subTable[offs].value = new Pair(key, value, null);
                n.subTable[offs].hash = h;
                n.value = null; //and scratch the old value field, just in case
                n.counter = 2;
            }
        }
    }    

    public ref V opIndex(K key)
    {
        Pair* pair = lookupPair(key);
        if(!pair)
            throw new RangeError("HashTrie - no such key");
        return pair.value;
    }

    public void remove(K key)
    {
        Node*[size_t.sizeof*8/STEP + 1] path;
        size_t h = typeid(K).getHash(&key);
        //TODO: need more info, i.e. true full path with slots
        size_t len = lookupLeafAndPath(h, root_, path.ptr);
        Node* n = path[len-1];
        if(n.value == null) // could have been here - but slot is empty
            return;
        if(n.hash != h) // the one in this slot has different hash
            return; 
        n.value = null;
        len--;
        while(len != 0){ //99.9% of cases, otherwise the root is leaf node
            Node* parent = path[len-1];
            parent.counter--;
            //anti flip-flop, only kill node if count == 0
            // don't try to convert back to leaf
            if(parent.counter != 0)
                break;
            len--;
        }
    }

    Pair* lookupPair(K key)
    {
        size_t h = typeid(K).getHash(&key);
        Node* p = lookupLeaf(h, root_);
        if(!p.value)
            return null;
        if(p.hash != h) //this leaf can as well have different hash
            return null;
        Pair* pair = p.value; //walk the collision chain - very rare event
        for(;;){
            if(pair.key == key)
                return pair;
            pair = pair.next;            
            if(pair == null)
                return null;
        }
    }

    // get to a leaf node for a given hash
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
}

unittest
{    
    auto trie = HashTrie!(int, int)(null);
    for(int i=1; i<=1000; i++){
        writeln(i);
        trie.insert(i, 10*i);
    }
    for(int i=1; i<=1000;i++)
        assert(trie[i] == i*10);
}