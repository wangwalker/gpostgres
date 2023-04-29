package ds

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// BtreeKey is the key of the B-tree. It contains metadata for btree node,
// name is used to compare the order of Keys, data is the wrapper of the
// metadata of the index.
type BtreeKey struct {
	Name string    `json:"n"`
	Data IndexData `json:"d"`
}

func (k BtreeKey) lt(other BtreeKey) bool {
	return k.Name < other.Name
}

func (k BtreeKey) IsEmpty() bool {
	return k.Name == "" && k.Data.Offset == 0 && k.Data.Length == 0
}

type BtreeNode struct {
	Keys     []BtreeKey   `json:"k"`
	Children []*BtreeNode `json:"c"`
	IsLeaf   bool         `json:"i"`
	Level    int          `json:"l"`
}

type Btree struct {
	Root *BtreeNode `json:"r"`
	// half of the degree of the B-tree
	degree int
	path   string
}

func NewBtree(d int) *Btree {
	return &Btree{Root: &BtreeNode{IsLeaf: true, Level: 1}, degree: d}
}

// AllNodes returns all nodes of the B-tree.
func (t *Btree) AllNodes() []*BtreeNode {
	nodes := make([]*BtreeNode, 0)
	r := t.Root
	nodes = append(nodes, r)
	for i := 0; i < len(r.Children); i++ {
		nodes = append(nodes, r.Children[i])
	}
	return nodes
}

// SetPath sets the path of the B-tree file.
func (tree *Btree) SetPath(path string) {
	if path == "" {
		return
	}
	tree.path = path
}

// Search key in the B-tree.
func (t *Btree) Search(k string) BtreeKey {
	return t.search(t.Root, k)
}

func (t *Btree) search(n *BtreeNode, k string) BtreeKey {
	i := 0
	for i < len(n.Keys) && k > n.Keys[i].Name {
		i++
	}
	if i < len(n.Keys) && k == n.Keys[i].Name {
		return n.Keys[i]
	}
	if n.IsLeaf {
		return BtreeKey{}
	}
	return t.search(n.Children[i], k)
}

// Insert inserts a key into the B-tree.
func (t *Btree) Insert(k BtreeKey) *BtreeNode {
	return t.insert(t.Root, k)
}

func (t *Btree) insert(n *BtreeNode, k BtreeKey) *BtreeNode {
	i := len(n.Keys) - 1
	if n.IsLeaf {
		n.Keys = append(n.Keys, k)
		j := i
		for ; j >= 0 && k.lt(n.Keys[j]); j-- {
			n.Keys[j+1] = n.Keys[j]
		}
		n.Keys[j+1] = k
		go t.flush()
		return n
	}
	for i >= 0 && k.lt(n.Keys[i]) {
		i--
	}
	i++
	if len(n.Children[i].Keys) == 2*t.degree-1 {
		t.splitChild(n, i)
		// recalculate the index after split node
		i = len(n.Keys) - 1
		for i >= 0 && k.lt(n.Keys[i]) {
			i--
		}
		i++
	}
	return t.insert(n.Children[i], k)
}

// Split node when the number of the keys = [2*t-1].
// In this case, first split the original child into two pieces with the
// middle key, then constuct a new node with the middle key and two children,
// finally insert the new node into the  parent node.
func (t *Btree) splitChild(parent *BtreeNode, i int) {
	// split original child into two pieces with the middle key,
	// child1, child2 = child[:t-1], child[t:]
	child := parent.Children[i]
	var child1, child2 *BtreeNode
	Level := child.Level + 1
	if child.IsLeaf {
		child1 = &BtreeNode{
			Keys:     child.Keys[:t.degree-1],
			Children: nil,
			IsLeaf:   child.IsLeaf,
			Level:    Level,
		}
		child2 = &BtreeNode{
			Keys:     child.Keys[t.degree:],
			Children: nil,
			IsLeaf:   child.IsLeaf,
			Level:    Level,
		}
	} else {
		child1 = &BtreeNode{
			Keys:     child.Keys[:t.degree-1],
			Children: child.Children[:t.degree-1],
			IsLeaf:   child.IsLeaf,
			Level:    Level,
		}
		child2 = &BtreeNode{
			Keys:     child.Keys[t.degree:],
			Children: child.Children[t.degree:],
			IsLeaf:   child.IsLeaf,
			Level:    Level,
		}
	}

	subParent := &BtreeNode{
		Keys:     []BtreeKey{child.Keys[t.degree-1]},
		Children: []*BtreeNode{child1, child2},
		IsLeaf:   false,
		Level:    child.Level,
	}
	parent.Children[i] = subParent
	t.merge(parent, subParent, i)
}

// Merge merges parent and child node when number of parent's keys < 2*t-1.
// Child is the new node after spliting, it has just one key and two children.
// It should be called after splitChild to balance tree.
func (t *Btree) merge(parent, child *BtreeNode, i int) {
	if len(parent.Keys) == 2*t.degree-1 {
		return
	}
	if i == 0 {
		parent.Keys = append(child.Keys, parent.Keys...)
		parent.Children = append(child.Children, parent.Children[1:]...)
	} else if len(parent.Keys) > i {
		// split parent's keys into two parts,
		// the middle one will be the only key at child node
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

func (t *Btree) traverse(n *BtreeNode) {
	fmt.Printf("level = %d, keys = %+v\n", n.Level, n.Keys)
	for i := range n.Children {
		if !n.IsLeaf && n.Children[i] != nil {
			t.traverse(n.Children[i])
		}
	}
}

// flush writes the btree node data to disk.
func (tree *Btree) flush() {
	path := tree.path
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("open index file failed: %v, path: %s\n", err, path)
		return
	}
	defer f.Close()
	// Truncate original content of the index file.
	if err := f.Truncate(0); err != nil {
		fmt.Printf("truncate index file failed: %v, path: %s\n", err, path)
		return
	}
	// TODO: Use effective way to update index file.
	bytes, err := json.Marshal(tree)
	if err != nil {
		fmt.Printf("marshal index failed: %v\n", err)
		return
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(bytes); err != nil {
		fmt.Printf("write index failed: %v\n", err)
		return
	}
	if err := w.Flush(); err != nil {
		fmt.Printf("flush index failed: %v\n", err)
	}
}
