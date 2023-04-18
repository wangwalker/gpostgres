package storage

// Index is all indexes of a table. As every column could has an index, so
// we can create multiple indexes for a table with a map, and the key is the
// column name, value is the index of the column when creating table.
// So by default, we will create indexes for all columns of a table.
type Index struct {
	name string // table name
	ids  map[string]*node
}

// NewIndex creates new index for table when creating.
func NewIndex(t Table) *Index {
	ids := make(map[string]*node)
	for _, c := range t.Columns {
		ids[string(c.Name)] = &node{isLeaf: true, level: 1}
	}
	return &Index{
		name: t.Name,
		ids:  ids,
	}
}

// Get gets the index of a column with the column name.
func (i Index) get(c string) *node {
	return i.ids[c]
}

// Insert inserts a key into the B-tree, f is the indexed field of a row.
// c is the column name of the table.
// n is the name of the column value.
// value is the row index.
func (index *Index) insert(c, n string, value uint16) {
	idx := index.get(c)
	if idx == nil {
		return
	}
	var b, p uint16
	idx.insert(key{name: n, value: value, page: p, block: b})
}

// Search searches a key in the B-tree index, f is the indexed field of a row.
// If the key is not found, it returns -1, otherwise it returns the row id.
func (index *Index) search(c string, f Field) key {
	idx := index.get(c)
	if idx == nil {
		return key{}
	}
	return idx.search(string(f))
}

// CreateIndex creates index for a table.
func (t *Table) createIndex() {
	t.index = NewIndex(*t)
}
