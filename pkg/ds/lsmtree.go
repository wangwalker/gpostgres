package ds

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

const (
	memtableSizeLimit = 2 * 1024 * 1024 // 2MB
	sstableSizeLimit  = 10 * 1024 * 1024
)

type sstable []*SkipListNode

// LSMTree is the data structure of LSM-Tree, it contains multiple levels of
// memtable and sstable, and the memtable is a skip list, the sstable is a
// sorted slice of node. The memtable is flushed to disk when it is full,
// and the sstable is merged when it is full.
type LSMTree struct {
	// c0 and c1 are the two levels of memtable, c0 is the current memtable,
	// c1 is the memtable which is flushing to disk.
	memtable          *SkipListNode
	sstable           sstable
	memtableSize      int
	memtableSizeLimit int
	sstableSizeLimit  int
	memtablePath      string
	sstablePath       string
}

// NewLSMTree returns a new LSM-Tree with empty memtable and sstable.
// For memtable, we can replace head node with first node when inserting.
func NewLSMTree(baseDir string) *LSMTree {
	return &LSMTree{
		memtable:          nil,
		sstable:           make([]*SkipListNode, 0),
		memtableSizeLimit: memtableSizeLimit,
		sstableSizeLimit:  sstableSizeLimit,
	}
}

// SetPath sets the path of the memtable file and sstable file, p1 is the path
// of memtable file, p2 is the path of sstable file.
func (tree *LSMTree) SetMemtablePath(p1, p2 string) {
	if p1 != "" {
		tree.memtablePath = p1
	}
	if p2 != "" {
		tree.sstablePath = p2
	}
}

// SetLimit sets the size limit of memtable and sstable, l1 is the size limit
// of memtable, l2 is the size limit of sstable.
func (tree *LSMTree) SetLimit(l1, l2 int) {
	if l1 > 0 {
		tree.memtableSizeLimit = l1
	}
	if l2 > 0 {
		tree.sstableSizeLimit = l2
	}
}

// Insert inserts the key and data into LSM-Tree, if the key is in memtable,
// the data is updated, otherwise the key and data is inserted into memtable.
// If the memtable is full, it is flushed to disk.
func (tree *LSMTree) Insert(k string, d IndexData) {
	if tree.memtable == nil {
		tree.memtable = NewSkipList(k, d)
		return
	}
	tree.memtable.Insert(k, d)
	tree.flushMemtable()
	tree.memtableSize += len(k) + d.size()
	if tree.memtableSize >= tree.memtableSizeLimit {
		tree.dumpMemtable()
	}
}

// Search searches the key in LSM-Tree, if the key is in memtable, the data is
// returned, otherwise the sstable is decoded from file and searched.
func (tree *LSMTree) Search(k string) IndexData {
	// phrase 1: search memtable
	if tree.memtable != nil {
		if d := tree.memtable.Search(k); !d.IsEmpty() {
			return d
		}
	}
	// phrase 2: search sstable
	if tree.sstable == nil {
		t := Decode(tree.sstablePath)
		if t == nil {
			return IndexData{}
		}
		tree.sstable = t
	}
	if d := tree.sstable.search(k); !d.IsEmpty() {
		return d
	}
	return IndexData{}
}

// flushMemtable flushes the memtable to disk every time inserts a new row.
func (tree *LSMTree) flushMemtable() {
	if tree.memtable == nil {
		return
	}
	f, err := os.OpenFile(tree.memtablePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	bytes, err := json.Marshal(tree.memtable)
	if err != nil {
		return
	}
	if err := tree.memtable.Write(w, bytes); err != nil {
		fmt.Printf("write memtable to disk failed: %v\n", err)
		return
	}
}

// dumpMemtable dumps the memtable to disk when its size reaches the limit,
// and creates a new memtable when finishes. If the sstable is empty, the
// memtable is dumped to disk directly, otherwise the memtable is merged with
// the sstable.
func (tree *LSMTree) dumpMemtable() {
	if tree.memtable == nil {
		return
	}
	f, err := os.OpenFile(tree.sstablePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	// merge sstable if it is not empty
	if fs, _ := f.Stat(); fs.Size() > 0 {
		tree.mergeSstable()
		return
	}
	// otherwise, construct sstable from memtable and dump memtable to disk
	w := bufio.NewWriter(f)
	nodes := tree.memtable.AllNodes()
	tree.sstable = nodes
	bytes, err := json.Marshal(nodes)
	if err != nil {
		return
	}
	if err := tree.memtable.Write(w, bytes); err != nil {
		fmt.Printf("write memtable to disk failed: %v\n", err)
		return
	}
	// create new memtable
	tree.memtable = nil
	tree.memtableSize = 0
}

// mergeSstable merges the sstable to disk, and creates a new sstable.
// TODO: merge sstable by size or by level
func (tree *LSMTree) mergeSstable() {
	c0 := tree.memtable.AllNodes()
	c1 := tree.sstable
	// merge c0 and c1 to c2
	c2 := make([]*SkipListNode, 0, len(c0)+len(c1))
	i, j := 0, 0
	for i < len(c0) && j < len(c1) {
		if c0[i].Key < c1[j].Key {
			c2 = append(c2, c0[i])
			i++
		} else {
			c2 = append(c2, c1[j])
			j++
		}
	}
	if i < len(c0) {
		c2 = append(c2, c0[i:]...)
	}
	if j < len(c1) {
		c2 = append(c2, c1[j:]...)
	}
	// dump c2 to disk
	f, err := os.OpenFile(tree.sstablePath, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.Seek(0, 0)
	w := bufio.NewWriter(f)
	bytes, err := json.Marshal(c2)
	if err != nil {
		return
	}
	if err := tree.memtable.Write(w, bytes); err != nil {
		fmt.Printf("write memtable to disk failed: %v\n", err)
		return
	}
	// create new memtable and sstable
	tree.memtable = nil
	tree.sstable = c2
	tree.memtableSize = 0
}

// search searches the key in sstable, if the key is found, the data is
// returned, otherwise the empty data is returned.
// TODO: use binary search to improve performance.
func (t sstable) search(k string) IndexData {
	for _, n := range t {
		if n.Key == k {
			return n.Data
		}
	}
	return IndexData{}
}
