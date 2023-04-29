package storage

import (
	"fmt"
	"os"

	"github.com/wangwalker/gpostgres/pkg/ds"
)

type indexType uint8

const (
	// indexTypeBtree is the btree index type.
	indexTypeBtree indexType = iota
	// indexTypeSkipList is the skip list index type.
	indexTypeLsmTree
)

const (
	btreeDir = "btree"
	lsmtDir  = "lsmt"
)

// Index is all indexes of a table. As every column could has an index, so
// we can create multiple indexes for a table with a map, and the key is the
// column name, value is the btree node of the column, when creating table.
// So by default, we will create indexes for all columns of a table.
type Index struct {
	Name     string                 `json:"n"` // table name
	Btrees   map[string]*ds.Btree   `json:"b"`
	LsmTrees map[string]*ds.LSMTree `json:"l"`
}

// NewIndex creates new index for table when creating.
func NewIndex(t Table) *Index {
	btrees := make(map[string]*ds.Btree)
	lsmtrees := make(map[string]*ds.LSMTree)
	for _, c := range t.Columns {
		cn := string(c.Name)
		bt := ds.NewBtree(2, path(indexTypeBtree, t.Name, cn))
		lsmt := ds.NewLSMTree(dir(indexTypeLsmTree, t.Name))
		btrees[cn] = bt
		lsmtrees[cn] = lsmt
	}
	return &Index{Name: t.Name, Btrees: btrees, LsmTrees: lsmtrees}
}

// dir returns the directory for a index type and table, tn is the table name.
func dir(t indexType, tn string) string {
	var subdir string
	switch t {
	case indexTypeBtree:
		subdir = btreeDir
	case indexTypeLsmTree:
		subdir = lsmtDir
	}
	return fmt.Sprintf("%s/%s/%s", config.IndexDir, subdir, tn)
}

// path returns the path of the index file for c column of tn table, t is the
// index type, could be btree or lsmtree.
func path(t indexType, tn, cn string) string {
	dir := dir(t, tn)
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}
	return fmt.Sprintf("%s/%s.index", dir, cn)
}

// getBtree gets the btree index of a column with the column name.
func (i Index) getBtree(c string) *ds.Btree {
	return i.Btrees[c]
}

// getLsmTree gets the lsmtree index of a column with the column name.
func (i Index) getLsmTree(c string) *ds.LSMTree {
	return i.LsmTrees[c]
}

// Insert inserts a key into the B-tree and lsmtree, c is the column name
// of the table,n is the name of the column value, p is page index, b is
// block index, and offset is byte offset in block.
// Note: p, b, offset and length should be calculated when inserting a new
// row into the avro binary file.
func (index *Index) insert(c, n string, offset, length, p, b uint16) {
	// first insert into lsmtree.
	lsmtree := index.getLsmTree(c)
	if lsmtree != nil {
		d := ds.IndexData{Offset: offset, Length: length, Page: p, Block: b}
		lsmtree.Insert(n, d)
	}
	// then insert into btree.
	btree := index.getBtree(c)
	if btree != nil {
		d := ds.IndexData{Offset: offset, Length: length, Page: p, Block: b}
		key := ds.BtreeKey{Name: n, Data: d}
		btree.Insert(key)
	}
}

// Search searches a key in the B-tree index, f is the indexed field of a row.
// If the key is not found, it returns empty, otherwise it returns index data.
func (index *Index) search(c string, f Field) ds.IndexData {
	btree := index.getBtree(c)
	if btree != nil {
		return btree.Search(string(f)).Data
	}
	lsmt := index.getLsmTree(c)
	if lsmt != nil {
		return lsmt.Search(string(f))
	}
	return ds.IndexData{}
}

// CreateIndex creates index for a table.
func (t *Table) createIndex() {
	t.index = NewIndex(*t)
}

// LoadIndex loads index from disk.
func (t *Table) loadIndex() {
	// create index for table, now index is empty.
	t.createIndex()
	// load index data from disk.
	for _, c := range t.Columns {
		cn := string(c.Name)
		if err := t.index.Btrees[cn].Load(); err != nil {
			panic(fmt.Sprintf("load btree index for column %s failed: %v", cn, err))
		}
		if err := t.index.LsmTrees[cn].Load(); err != nil {
			panic(fmt.Sprintf("load lsmtree index for column %s failed: %v", cn, err))
		}
	}
}
