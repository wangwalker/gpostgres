package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/wangwalker/gpostgres/pkg/ds"
)

// Index is all indexes of a table. As every column could has an index, so
// we can create multiple indexes for a table with a map, and the key is the
// column name, value is the btree node of the column, when creating table.
// So by default, we will create indexes for all columns of a table.
type Index struct {
	Name    string               `json:"n"` // table name
	Btrees  map[string]*ds.Btree `json:"b"`
	writers map[string]*bufio.Writer
	ticker  *time.Ticker
}

// NewIndex creates new index for table when creating.
func NewIndex(t Table) *Index {
	btrees := make(map[string]*ds.Btree)
	for _, c := range t.Columns {
		cn := string(c.Name)
		btrees[cn] = ds.NewBtree(2)
	}
	i := &Index{Name: t.Name, Btrees: btrees}
	for _, c := range t.Columns {
		cn := string(c.Name)
		btrees[cn].SetPath(i.path(cn))
	}
	return i
}

// Path returns the path of the index file for a column.
func (i Index) path(c string) string {
	dir := fmt.Sprintf("%s/%s", config.IndexDir, i.Name)
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}
	return fmt.Sprintf("%s/%s/%s.index", config.IndexDir, i.Name, c)
}

// Get gets the index of a column with the column name.
func (i Index) get(c string) *ds.Btree {
	return i.Btrees[c]
}

// Insert inserts a key into the B-tree, f is the indexed field of a row.
// c is the column name of the table,n is the name of the column value,
// p is page index, b is block index, and offset is byte offset in block.
// Note: p, b, offset and length should be calculated when inserting a new
// row into the avro binary file.
func (index *Index) insert(c, n string, offset, length, p, b uint16) {
	btree := index.get(c)
	if btree == nil {
		return
	}
	d := ds.IndexData{Offset: offset, Length: length, Page: p, Block: b}
	key := ds.BtreeKey{Name: n, Data: d}
	btree.Insert(key)
}

// Search searches a key in the B-tree index, f is the indexed field of a row.
// If the key is not found, it returns -1, otherwise it returns the row id.
func (index *Index) search(c string, f Field) ds.BtreeKey {
	btree := index.get(c)
	if btree == nil {
		return ds.BtreeKey{}
	}
	return btree.Search(string(f))
}

// CreateIndex creates index for a table.
func (t *Table) createIndex() {
	t.index = NewIndex(*t)
	// start a ticker to flush index to disk periodically.
	t.index.ticker = time.NewTicker(time.Second * 5)
	defer t.index.ticker.Stop()
	go func() {
		for range t.index.ticker.C {
			for _, w := range t.index.writers {
				// TODO: reopen file as it has been closed by defer when insert.
				if err := w.Flush(); err != nil {
					fmt.Printf("flush index failed: %v\n", err)
					return
				}
			}
		}
	}()
}

// LoadIndex loads index from disk.
func (t *Table) loadIndex() {
	// create index for table, now index is empty.
	t.createIndex()
	// load index data from disk.
	for _, c := range t.Columns {
		cn := string(c.Name)
		path := t.index.path(cn)
		f, err := os.Open(path)
		if err != nil && os.IsNotExist(err) {
			return
		}
		defer f.Close()

		var btree ds.Btree
		if err := json.NewDecoder(f).Decode(&btree); err != nil {
			fmt.Printf("decode index failed: %v\n", err)
			return
		}
		if btree.Root == nil || btree.Root.Keys == nil || len(btree.Root.Keys) <= 0 {
			continue
		}
		t.index.Btrees[cn] = &btree
	}
}
