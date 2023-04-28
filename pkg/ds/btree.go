package ds

import (
	"fmt"
)

// Using t to represent the half of degree of the B-tree,
// which means the maximum number of Keys in a node is 2t-1.
var t = 2

// Key is the key of the B-tree. It contains metadata for the btree node,
// name is used to compare the order of Keys, value is used to store other
// information, such as the offset or id of the row, page and block is the
// position of the row in the local binary file.
// Note: we just put all indexes of one column in one file, so every time
// inserting a new row, we need to update index file of the column. In order
// to improve performance, we can update file with buffers, and flush the
// buffers to disk when the buffer is full.
type BtreeKey struct {
	Name   string `json:"n"`
	Offset uint16 `json:"v"`
	Length uint16 `json:"l"`
	Page   uint16 `json:"p"`
	Block  uint16 `json:"b"`
}

func (k BtreeKey) lt(other BtreeKey) bool {
	return k.Name < other.Name
}

func (k BtreeKey) IsEmpty() bool {
	return k.Name == "" && k.Offset == 0
}

type BtreeNode struct {
	Keys     []BtreeKey   `json:"k"`
	Children []*BtreeNode `json:"c"`
	IsLeaf   bool         `json:"i"`
	Level    int          `json:"l"`
}

// Config configures the B-tree.
func (tree *BtreeNode) Config(degree int) {
	if degree < 2 {
		degree = 2
	}
	t = degree
}

// Search key in the B-tree.
func (tree *BtreeNode) Search(n string) BtreeKey {
	i := 0
	for i < len(tree.Keys) && n > tree.Keys[i].Name {
		i++
	}
	if i < len(tree.Keys) && n == tree.Keys[i].Name {
		return tree.Keys[i]
	}
	if tree.IsLeaf {
		return BtreeKey{}
	}
	return tree.Children[i].Search(n)
}

// Insert inserts a key into the B-tree, which is the outer interface.
func (tree *BtreeNode) Insert(k BtreeKey) *BtreeNode {
	i := len(tree.Keys) - 1
	if tree.IsLeaf {
		tree.Keys = append(tree.Keys, k)
		j := i
		for ; j >= 0 && k.lt(tree.Keys[j]); j-- {
			tree.Keys[j+1] = tree.Keys[j]
		}
		tree.Keys[j+1] = k
		return tree
	}
	for i >= 0 && k.lt(tree.Keys[i]) {
		i--
	}
	i++
	if len(tree.Children[i].Keys) == 2*t-1 {
		tree.splitChild(i, tree.Children[i])
		// recalculate the index after split node
		i = len(tree.Keys) - 1
		for i >= 0 && k.lt(tree.Keys[i]) {
			i--
		}
		i++
	}
	return tree.Children[i].Insert(k)
}

// Split node when the number of the keys = [2*t-1].
// In this case, first split the original child into two pieces with the
// middle key, then constuct a new node with the middle key and two children,
// finally insert the new node into the  parent node.
func (parent *BtreeNode) splitChild(i int, child *BtreeNode) {
	// split original child into two pieces with the middle key,
	// child1, child2 = child[:t-1], child[t:]
	var child1, child2 *BtreeNode
	Level := child.Level + 1
	if child.IsLeaf {
		child1 = &BtreeNode{
			Keys:     child.Keys[:t-1],
			Children: nil,
			IsLeaf:   child.IsLeaf,
			Level:    Level,
		}
		child2 = &BtreeNode{
			Keys:     child.Keys[t:],
			Children: nil,
			IsLeaf:   child.IsLeaf,
			Level:    Level,
		}
	} else {
		child1 = &BtreeNode{
			Keys:     child.Keys[:t-1],
			Children: child.Children[:t-1],
			IsLeaf:   child.IsLeaf,
			Level:    Level,
		}
		child2 = &BtreeNode{
			Keys:     child.Keys[t:],
			Children: child.Children[t:],
			IsLeaf:   child.IsLeaf,
			Level:    Level,
		}
	}

	subParent := &BtreeNode{
		Keys:     []BtreeKey{child.Keys[t-1]},
		Children: []*BtreeNode{child1, child2},
		IsLeaf:   false,
		Level:    child.Level,
	}
	parent.Children[i] = subParent
	parent.merge(subParent, i)
}

// Merge merges parent and child node when the number of parent's keys < 2*t-1.
// Child node is the new node after spliting, so it has just one key and two children.
// It should be called after splitChild to balance tree.
func (parent *BtreeNode) merge(child *BtreeNode, i int) {
	if len(parent.Keys) == 2*t-1 {
		return
	}
	if i == 0 {
		parent.Keys = append(child.Keys, parent.Keys...)
		parent.Children = append(child.Children, parent.Children[1:]...)
	} else if len(parent.Keys) > i {
		// split parent's Keys into two pieces, the middle one will be the only key at child node
		k1, k2 := parent.Keys[:i], parent.Keys[i:]
		Keys := make([]BtreeKey, 0, len(k1)+len(k2)+1)
		Keys = append(Keys, k1...)
		Keys = append(Keys, child.Keys[0])
		Keys = append(Keys, k2...)
		parent.Keys = Keys
		// split parent children into two pieces, will ignore the middle one
		c1, c2 := parent.Children[:i], parent.Children[i+1:]
		Children := make([]*BtreeNode, 0, len(c1)+len(c2)+1)
		Children = append(Children, c1...)
		Children = append(Children, child.Children...)
		Children = append(Children, c2...)
		parent.Children = Children
	} else {
		// just append the key and children of child node to parent
		parent.Keys = append(parent.Keys, child.Keys[0])
		parent.Children = append(parent.Children[:i], child.Children...)
	}
	// update Level of children
	for _, c := range parent.Children {
		c.Level = parent.Level + 1
	}
}

func traverse(tree *BtreeNode) {
	fmt.Printf("level = %d, keys = %+v\n", tree.Level, tree.Keys)
	for i := range tree.Children {
		if !tree.IsLeaf && tree.Children[i] != nil {
			traverse(tree.Children[i])
		}
	}
}
