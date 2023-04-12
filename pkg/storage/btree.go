package storage

import (
	"fmt"
)

// Using t to represent the half of degree of the B-tree,
// which means the maximum number of keys in a node is 2t-1.
const t = 2

// key is the key of the B-tree. It contains metadata for the btree node,
// name is used to compare the order of keys, value is used to store other information,
// such as the offset or id of the row.
type key struct {
	name  string
	value int32
}

func (k key) lt(other key) bool {
	return k.name < other.name
}

type node struct {
	keys     []key
	children []*node
	isLeaf   bool
	level    int
}

// Search key in the B-tree.
func (tree *node) search(k string) int32 {
	i := 0
	for i < len(tree.keys) && k > tree.keys[i].name {
		i++
	}
	if i < len(tree.keys) && k == tree.keys[i].name {
		return tree.keys[i].value
	}
	if tree.isLeaf {
		return -1
	}
	return tree.children[i].search(k)
}

// Insert inserts a key into the B-tree, which is the outer interface.
func (tree *node) insert(k key) *node {
	i := len(tree.keys) - 1
	if tree.isLeaf {
		tree.keys = append(tree.keys, k)
		j := i
		for ; j >= 0 && k.lt(tree.keys[j]); j-- {
			tree.keys[j+1] = tree.keys[j]
		}
		tree.keys[j+1] = k
		return tree
	}
	for i >= 0 && k.lt(tree.keys[i]) {
		i--
	}
	i++
	if len(tree.children[i].keys) == 2*t-1 {
		tree.splitChild(i, tree.children[i])
		// recalculate the index after split node
		i = len(tree.keys) - 1
		for i >= 0 && k.lt(tree.keys[i]) {
			i--
		}
		i++
	}
	return tree.children[i].insert(k)
}

// Split node when the number of the keys = [2*t-1].
// In this case, first split the original child into two pieces with the
// middle key, then constuct a new node with the middle key and two children,
// finally insert the new node into the  parent node.
func (parent *node) splitChild(i int, child *node) {
	// split original child into two pieces with the middle key,
	// child1, child2 = child[:t-1], child[t:]
	var child1, child2 *node
	level := child.level + 1
	if child.isLeaf {
		child1 = &node{
			keys:     child.keys[:t-1],
			children: nil,
			isLeaf:   child.isLeaf,
			level:    level,
		}
		child2 = &node{
			keys:     child.keys[t:],
			children: nil,
			isLeaf:   child.isLeaf,
			level:    level,
		}
	} else {
		child1 = &node{
			keys:     child.keys[:t-1],
			children: child.children[:t-1],
			isLeaf:   child.isLeaf,
			level:    level,
		}
		child2 = &node{
			keys:     child.keys[t:],
			children: child.children[t:],
			isLeaf:   child.isLeaf,
			level:    level,
		}
	}

	subParent := &node{
		keys:     []key{child.keys[t-1]},
		children: []*node{child1, child2},
		isLeaf:   false,
		level:    child.level,
	}
	parent.children[i] = subParent
	parent.merge(subParent, i)
}

// Merge merges parent and child node when the number of parent's keys < 2*t-1.
// Child node is the new node after spliting, so it has just one key and two children.
// It should be called after splitChild to balance tree.
func (parent *node) merge(child *node, i int) {
	if len(parent.keys) == 2*t-1 {
		return
	}
	if i == 0 {
		parent.keys = append(child.keys, parent.keys...)
		parent.children = append(child.children, parent.children[1:]...)
	} else if len(parent.keys) > i {
		// split parent's keys into two pieces, the middle one will be the only key at child node
		k1, k2 := parent.keys[:i], parent.keys[i:]
		keys := make([]key, 0, len(k1)+len(k2)+1)
		keys = append(keys, k1...)
		keys = append(keys, child.keys[0])
		keys = append(keys, k2...)
		parent.keys = keys
		// split parent children into two pieces, will ignore the middle one
		c1, c2 := parent.children[:i], parent.children[i+1:]
		children := make([]*node, 0, len(c1)+len(c2)+1)
		children = append(children, c1...)
		children = append(children, child.children...)
		children = append(children, c2...)
		parent.children = children
	} else {
		// just append the key and children of child node to parent
		parent.keys = append(parent.keys, child.keys[0])
		parent.children = append(parent.children[:i], child.children...)
	}
	// update level of children
	for _, c := range parent.children {
		c.level = parent.level + 1
	}
}

func traverse(tree *node) {
	fmt.Printf("level = %d, keys = %+v\n", tree.level, tree.keys)
	for i := range tree.children {
		if !tree.isLeaf && tree.children[i] != nil {
			traverse(tree.children[i])
		}
	}
}
