package ds

import (
	"bufio"
	"encoding/json"
	"io"
	"math/rand"
	"os"

	"github.com/wangwalker/dsal/linear"
)

// IndexData is the metadata of the node, offset is the byte offset of the
// row in the avro binary file, length is the length of the row, page is the
// page index of the row in the avro binary file, block is the block index
// of the row in the avro binary file.
type IndexData struct {
	Offset uint16 `json:"v"`
	Length uint16 `json:"l"`
	Page   uint16 `json:"p"`
	Block  uint16 `json:"b"`
}

// IsEmpty returns true if the IndexData is empty, otherwise false.
func (d IndexData) IsEmpty() bool {
	return d.Offset == 0 && d.Length == 0
}

// size returns the size of the IndexData.
func (d IndexData) size() int {
	return 8
}

// SkipListNode is the node of skip list, key is the key of the node, data is
// the metadata of the index. The skip list is a linked list with multiple
// levels, the top level is the head node, the bottom level is the tail node.
// Right is the next pointer in the linked list with same level, down is the
// next pointer at the next level.
type SkipListNode struct {
	Key   string        `json:"k"`
	Data  IndexData     `json:"a"`
	Right *SkipListNode `json:"r"`
	Down  *SkipListNode `json:"d"`
	// dicision maker for inserting at next level, default is RandomDicisionMaker,
	// which is global shared for head node, can be set by SetDicisionMaker method.
	dm DicisionMaker
}

// NewSkipList returns a new skip list with the given key and data as the head
// node of the top level.
func NewSkipList(k string, d IndexData) *SkipListNode {
	return &SkipListNode{
		Key:   k,
		Data:  d,
		Right: nil, Down: nil,
		dm: &RandomDicisionMaker{},
	}
}

// SetDicisionMaker sets the dicision maker for the skip list, the dicision
// maker is used when inserting a new node to decide whether to insert at next
// level or not.
func (head *SkipListNode) SetDicisionMaker(dm DicisionMaker) {
	head.dm = dm
}

// Search searches the target key in the skip list, if the key is found, the
// data of the node is returned, otherwise emtpy is returned.
func (head *SkipListNode) Search(k string) IndexData {
	if head != nil && k == head.Key {
		return head.Data
	}
	p := head
	for p != nil {
		if p.Right == nil || p.Right.Key > k {
			p = p.Down
		} else if p.Right.Key == k {
			return p.Right.Data
		} else {
			p = p.Right
		}
	}
	return IndexData{}
}

// Insert inserts the key and data into skip list when the key is not in
// the skip list, otherwise updates the value of the Key.
func (head *SkipListNode) Insert(k string, d IndexData) *SkipListNode {
	// trace the path when searching the Key
	path := linear.NewStack()
	p := head
	for p != nil {
		for p.Right != nil && p.Right.Key < k {
			p = p.Right
		}
		path.Push(p)
		p = p.Down
	}
	var down *SkipListNode
	shouldInsert := true
	for shouldInsert && !path.Empty() {
		insert, _ := path.Pop().(*SkipListNode)
		insert.Right = &SkipListNode{Key: k, Data: d, Right: insert.Right, Down: down}
		// record for next iteration
		down = insert.Right
		// decide whether to insert at next level
		shouldInsert = head.dm.ShouldInsert()
	}
	// finally, insert at the new top level if needed
	if shouldInsert {
		// create the new right node at the most top level
		Right := &SkipListNode{Key: k, Data: down.Data, Right: nil, Down: down}
		// create the new head node at the most top level
		head = &SkipListNode{Key: head.Key, Data: head.Data, Right: Right, Down: head, dm: head.dm}
	}
	return head
}

// Update updates the value of the Key in the skip list, if the key is not in
// the skip list, nothing happens.
func (head *SkipListNode) Update(k string, d IndexData) {
	if head.Search(k).IsEmpty() {
		return
	}
	p := head
	h := head
	for p != nil {
		if p.Right == nil {
			h = h.Down
			p = h
		} else {
			if p.Key == k {
				p.Data = d
				p = p.Down
			} else {
				p = p.Right
			}
		}
	}
}

// Delete deletes the node from the skip list, if the node is not in the skip list,
// nothing happens.
func (head *SkipListNode) Delete(k string) {
	p := head
	for p != nil {
		for p.Right != nil && p.Right.Key < k {
			p = p.Right
		}
		if p.Right == nil || p.Right.Key > k {
			p = p.Down
		} else {
			p.Right = p.Right.Right
			p = p.Down
		}
	}
}

// AllNodes returns all nodes of the skip list.
func (head *SkipListNode) AllNodes() []*SkipListNode {
	nodes := make([]*SkipListNode, 0)
	p := head
	for p != nil && p.Down != nil {
		p = p.Down
	}
	nodes = append(nodes, p)
	for p.Right != nil {
		nodes = append(nodes, p.Right)
		p = p.Right
	}
	return nodes
}

// Write just writes the unique all node of skip list to the file.
func (head *SkipListNode) Write(w io.Writer, bytes []byte) error {
	if head == nil {
		return nil
	}
	if w == nil || bytes == nil || len(bytes) == 0 {
		return nil
	}
	_, err := w.Write(bytes)
	return err
}

// Read reads the unique all node of skip list from the file and inserts them
// into the skip list.
func (head *SkipListNode) Read(r io.Reader) error {
	var nodes []*SkipListNode
	dec := json.NewDecoder(r)
	err := dec.Decode(&nodes)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		head.Insert(n.Key, n.Data)
	}
	return nil
}

// Decode decodes the sstable from the file encoded by json.
func Decode(p string) []*SkipListNode {
	f, err := os.Open(p)
	if err != nil {
		return nil
	}
	defer f.Close()
	r := bufio.NewReader(f)
	dec := json.NewDecoder(r)
	var t []*SkipListNode
	err = dec.Decode(&t)
	if err != nil {
		return nil
	}
	return t
}

// DicisionMaker is the interface for making dicision about wheather to insert
// at next level or not when inserting a new node. If returns true, the new node
// will be inserted at next level, otherwise not.
type DicisionMaker interface {
	ShouldInsert() bool
}

// RandomDicisionMaker is the default dicision maker, it randomly returns true
// or false.
type RandomDicisionMaker struct{}

// ShouldInsert implements the ShouldInsert method of DicisionMaker interface.
func (r *RandomDicisionMaker) ShouldInsert() bool {
	return rand.Intn(2) == 0
}
