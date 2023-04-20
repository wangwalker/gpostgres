package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Index is all indexes of a table. As every column could has an index, so
// we can create multiple indexes for a table with a map, and the key is the
// column name, value is the btree node of the column, when creating table.
// So by default, we will create indexes for all columns of a table.
type Index struct {
	Name    string           `json:"n"` // table name
	Btrees  map[string]*node `json:"b"`
	writers map[string]*bufio.Writer
	ticker  *time.Ticker
}

// NewIndex creates new index for table when creating.
func NewIndex(t Table) *Index {
	btrees := make(map[string]*node)
	for _, c := range t.Columns {
		btrees[string(c.Name)] = &node{IsLeaf: true, Level: 1}
	}
	return &Index{
		Name:   t.Name,
		Btrees: btrees,
	}
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
func (i Index) get(c string) *node {
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
	btree.insert(key{Name: n, Offset: offset, Length: length, Page: p, Block: b})
	// update index file
	path := index.path(c)
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
	bytes, err := json.Marshal(index.Btrees[c])
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

// Search searches a key in the B-tree index, f is the indexed field of a row.
// If the key is not found, it returns -1, otherwise it returns the row id.
func (index *Index) search(c string, f Field) key {
	btree := index.get(c)
	if btree == nil {
		return key{}
	}
	return btree.search(string(f))
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
		path := t.index.path(string(c.Name))
		f, err := os.Open(path)
		if err != nil && os.IsNotExist(err) {
			return
		}
		defer f.Close()

		var btree node
		if err := json.NewDecoder(f).Decode(&btree); err != nil {
			fmt.Printf("decode index failed: %v\n", err)
			return
		}
		t.index.Btrees[string(c.Name)] = &btree
	}
}
