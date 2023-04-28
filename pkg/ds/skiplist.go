package ds

import (
	"math/rand"

	"github.com/wangwalker/dsal/linear"
)

// SkipListNode is the node of skip list, key and value are the data of the node,
// right is the next pointer in the linked list with same level, down is the
// next pointer at the next level.
type SkipListNode struct {
	key   string
	value uint16
	right *SkipListNode
	down  *SkipListNode
	// dicision maker for inserting at next level, default is RandomDicisionMaker,
	// which is global shared for head node, can be set by SetDicisionMaker method.
	dm DicisionMaker
}

// NewSkipList returns a new skip list with the given key and value as the head
// node of the top level.
func NewSkipList(key string, value uint16) *SkipListNode {
	return &SkipListNode{key: key, value: value, right: nil, down: nil, dm: &RandomDicisionMaker{}}
}

// SetDicisionMaker sets the dicision maker for the skip list, the dicision
// maker is used when inserting a new node to decide whether to insert at next
// level or not.
func (head *SkipListNode) SetDicisionMaker(dm DicisionMaker) {
	head.dm = dm
}

// Search searches the target key in the skip list, if the key is found, the
// value of the node is returned, otherwise 0 is returned.
func (head *SkipListNode) Search(key string) uint16 {
	if head != nil && key == head.key {
		return head.value
	}
	p := head
	for p != nil {
		if p.right == nil || p.right.key > key {
			p = p.down
		} else if p.right.key == key {
			return p.right.value
		} else {
			p = p.right
		}
	}
	return 0
}

// Insert inserts the key and value into the skip list when the key is not in
// the skip list, otherwise updates the value of the key.
func (head *SkipListNode) Insert(key string, value uint16) *SkipListNode {
	// trace the path when searching the key
	path := linear.NewStack()
	p := head
	for p != nil {
		for p.right != nil && p.right.key < key {
			p = p.right
		}
		path.Push(p)
		p = p.down
	}
	var down *SkipListNode
	shouldInsert := true
	for shouldInsert && !path.Empty() {
		insert, _ := path.Pop().(*SkipListNode)
		insert.right = &SkipListNode{key: key, value: value, right: insert.right, down: down}
		// record for next iteration
		down = insert.right
		// decide whether to insert at next level
		shouldInsert = head.dm.ShouldInsert()
	}
	// finally, insert at the new top level if needed
	if shouldInsert {
		// create the new right node at the most top level
		right := &SkipListNode{key: key, value: value, right: nil, down: down}
		// create the new head node at the most top level
		head = &SkipListNode{key: head.key, value: head.value, right: right, down: head, dm: head.dm}
	}
	return head
}

// Update updates the value of the key in the skip list, if the key is not in
// the skip list, nothing happens.
func (head *SkipListNode) Update(key string, value uint16) {
	if head.Search(key) == 0 {
		return
	}
	p := head
	h := head
	for p != nil {
		if p.right == nil {
			h = h.down
			p = h
		} else {
			if p.key == key {
				p.value = value
				p = p.down
			} else {
				p = p.right
			}
		}
	}
}

// Delete deletes the key from the skip list, if the key is not in the skip list,
// nothing happens.
func (head *SkipListNode) Delete(key string) {
	p := head
	for p != nil {
		for p.right != nil && p.right.key < key {
			p = p.right
		}
		if p.right == nil || p.right.key > key {
			p = p.down
		} else {
			p.right = p.right.right
			p = p.down
		}
	}
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
