package storage

import "fmt"

// Using t to represent the half of degree of the B-tree,
// which means the maximum number of keys in a node is 2t-1.
const t = 2

type node struct {
	keys     []int
	children []*node
	isLeaf   bool
}

// Search key in the B-tree.
func search(n *node, key int) *node {
	i := 0
	for i < len(n.keys) && key > n.keys[i] {
		i++
	}
	if i < len(n.keys) && key == n.keys[i] {
		return n
	}
	if n.isLeaf {
		return nil
	}
	return search(n.children[i], key)
}

// Insert inserts a key into the B-tree, which is the outer interface.
func insert(n *node, key int) *node {
	if len(n.keys) == 2*t-1 {
		newRoot := &node{
			keys:     []int{n.keys[t-1]},
			children: []*node{n, nil},
			isLeaf:   false,
		}

		splitChild(newRoot, 1, n)
		insertNonFull(newRoot, key)
		return newRoot
	}
	return insertNonFull(n, key)
}

// Split node when the number of the keys = [2*t-1].
// In this case, first split the original child into two pieces with the
// middle key, then constuct a new node with the middle key and two children,
// finally insert the new node into the  parent node.
func splitChild(parent *node, i int, child *node) {
	// split original child into two pieces with the middle key,
	// child1, child2 = child[:t-1], child[t:]
	var child1, child2 *node
	if child.isLeaf {
		child1 = &node{
			keys:     child.keys[:t-1],
			children: nil,
			isLeaf:   child.isLeaf,
		}
		child2 = &node{
			keys:     child.keys[t:],
			children: nil,
			isLeaf:   child.isLeaf,
		}
	} else {
		child1 = &node{
			keys:     child.keys[:t-1],
			children: child.children[:t-1],
			isLeaf:   child.isLeaf,
		}
		child2 = &node{
			keys:     child.keys[t:],
			children: child.children[t:],
			isLeaf:   child.isLeaf,
		}
	}

	subParent := &node{
		keys:     []int{child.keys[t-1]},
		children: []*node{child1, child2},
		isLeaf:   false,
	}
	parent.children[i] = subParent
}

// Insert a key into a non-full node.
func insertNonFull(n *node, key int) *node {
	i := len(n.keys) - 1
	if n.isLeaf {
		n.keys = append(n.keys, 0)
		j := i
		for ; j >= 0 && key < n.keys[j]; j-- {
			n.keys[j+1] = n.keys[j]
		}
		n.keys[j+1] = key
		return n
	}
	for i >= 0 && key < n.keys[i] {
		i--
	}
	i++
	if len(n.children[i].keys) == 2*t-1 {
		splitChild(n, i, n.children[i])
	}
	return insertNonFull(n.children[i], key)
}

// Traverse the B-tree.
var level = 0

func traverse(n *node) {
	fmt.Printf("level = %d, keys = %+v\n", level, n.keys)
	for i := range n.children {
		if !n.isLeaf && n.children[i] != nil {
			traverse(n.children[i])
		}
	}
}

func test() {
	root := &node{
		keys: []int{50, 100},
		children: []*node{
			{keys: []int{10, 20, 30}, isLeaf: true},
			{keys: []int{60, 70, 80}, isLeaf: true},
			{keys: []int{110, 120, 130}, isLeaf: true}},
		isLeaf: false,
	}

	traverse(root)

	_ = insert(root, 40)
	_ = insert(root, 140)
	_ = insert(root, 75)

	traverse(root)

	c1 := search(root, 40)
	c2 := search(root, 140)
	c3 := search(root, 75)
	c4 := search(root, 90)
	fmt.Println(c1, c2, c3, c4)
}
