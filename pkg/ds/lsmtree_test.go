package ds

import (
	"fmt"
	"os"
	"testing"
)

func TestNewLSMTree(t *testing.T) {
	dir := fmt.Sprintf("%s/lsmd1", testDir)
	tree := NewLSMTree(dir)
	if tree == nil {
		t.Errorf("tree should not be nil")
	}
	if tree != nil && tree.memtable != nil {
		t.Errorf("tree memtable should be nil")
	}
	if tree != nil && tree.sstable == nil {
		t.Errorf("tree sstable should not be nil")
	}
	if tree != nil && tree.memtableSizeLimit != memtableSizeLimit {
		t.Errorf("tree memtable size limit is not correct")
	}
	if tree != nil && tree.sstableSizeLimit != sstableSizeLimit {
		t.Errorf("tree sstable size limit is not correct")
	}
	if tree.memtablePath == "" || tree.sstablePath == "" {
		t.Errorf("tree memtable path or sstable path should not be empty")
	}
}

func TestSetMemtablePath(t *testing.T) {
	dir := fmt.Sprintf("%s/lsmd2", testDir)
	tree := NewLSMTree(dir)
	tree.SetMemtablePath("p1", "p2")
	if tree.memtablePath != fmt.Sprintf("%s/p1", dir) {
		t.Errorf("tree memtable path is not correct")
	}
	if tree.sstablePath != fmt.Sprintf("%s/p2", dir) {
		t.Errorf("tree sstable path is not correct")
	}
}

func TestSetLimit(t *testing.T) {
	dir := fmt.Sprintf("%s/lsmd3", testDir)
	tree := NewLSMTree(dir)
	tree.SetLimit(10, 20)
	if tree.memtableSizeLimit != 10 {
		t.Errorf("tree memtable size limit is not correct")
	}
	if tree.sstableSizeLimit != 20 {
		t.Errorf("tree sstable size limit is not correct")
	}
}

func TestInsertOneKey(t *testing.T) {
	dir := fmt.Sprintf("%s/lsmd4", testDir)
	tree := NewLSMTree(dir)
	tree.Insert("key1", IndexData{})
	tree.Insert("key2", IndexData{})
	if tree.memtable == nil {
		t.Errorf("tree memtable should not be nil")
	}
	if tree.memtable != nil && tree.memtable.Key != "key1" {
		t.Errorf("tree memtable key is not correct")
	}
	if tree.memtable != nil && tree.memtable.Data.Offset != 0 {
		t.Errorf("tree memtable data is not correct")
	}
	_, err := os.Open(tree.memtablePath)
	if err != nil {
		t.Errorf("tree memtable file should be created")
	}
}

func TestInsertManyKeysToFlushSSTable(t *testing.T) {
	// GIVEN
	dir := fmt.Sprintf("%s/lsmd5", testDir)
	tree := NewLSMTree(dir)

	// WHEN
	// key is 2 bytes, data is 8 bytes, so every node is 10 bytes.
	// memtable size limit is 100 bytes, so 10 nodes will make memtable full.
	tree.SetLimit(100, 200)
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i+1)
		tree.Insert(k, IndexData{Offset: uint16(10 * i)})
	}

	// THEN
	if tree.memtable != nil {
		t.Errorf("tree memtable should be nil")
	}
	if tree.memtableSize != 0 {
		t.Errorf("tree memtable size is not correct")
	}
	if len(tree.sstable) != 10 {
		t.Errorf("tree sstable size is not correct")
	}
}

func TestSearchLSMTree(t *testing.T) {
	// GIVEN
	dir := fmt.Sprintf("%s/lsmd6", testDir)
	tree := NewLSMTree(dir)
	tree.SetLimit(100, 200)
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i+1)
		tree.Insert(k, IndexData{Offset: uint16(10 * i)})
	}

	// WHEN
	d := tree.Search("k5")

	// THEN
	if d.Offset != 40 {
		t.Errorf("tree search result is not correct")
	}
}

func TestMergeSSTable(t *testing.T) {
	// GIVEN
	dir := fmt.Sprintf("%s/lsmd7", testDir)
	tree := NewLSMTree(dir)
	n := 10
	for i := 0; i < n; i++ {
		idx := (i + 1) * 2
		k1 := fmt.Sprintf("k%d", idx-1)
		k2 := fmt.Sprintf("k%d", idx)
		d1 := IndexData{Offset: uint16(10 * (idx - 1))}
		d2 := IndexData{Offset: uint16(10 * idx)}
		tree.Insert(k1, d1)
		tree.sstable = append(tree.sstable, NewSkipList(k2, d2))
	}

	// WHEN
	tree.mergeSstable()

	// THEN
	if tree.memtable != nil {
		t.Errorf("tree memtable should be nil")
	}
	if tree.memtableSize != 0 {
		t.Errorf("tree memtable size is not correct")
	}
	if len(tree.sstable) != 2*n {
		t.Errorf("tree sstable size is not correct")
	}
}
