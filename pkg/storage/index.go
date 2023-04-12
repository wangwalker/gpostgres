package storage

type Index struct {
	Name string // column name of table
	tree *node
}

// NewIndex creates a new index for a table.
func NewIndex(name string) *Index {
	return &Index{
		Name: name,
		tree: &node{isLeaf: true, level: 1},
	}
}

// Insert inserts a key into the B-tree, f is the indexed field of a row.
func (index *Index) Insert(name string, value int32) {
	var b, p uint8
	index.tree.insert(key{name: name, value: value, page: p, block: b})
}

// Search searches a key in the B-tree, f is the indexed field of a row.
func (index *Index) Search(f Field) int32 {
	return index.tree.search(string(f))
}

// CreateIndex creates a new index for a table.
func (t *Table) CreateIndex(name string) {
	t.Indexes = append(t.Indexes, *NewIndex(name))
}

// Index returns the index of a table by name.
func (t *Table) Index(name string) *Index {
	for _, i := range t.Indexes {
		if i.Name == name {
			return &i
		}
	}
	return nil
}
