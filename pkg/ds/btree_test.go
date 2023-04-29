package ds

import (
	"os"
	"testing"
)

// Used for testing when manipulating files
const testDir = "tempstorage"

func init() {
	// if the testDir exists, if so, remove it and recreate it
	os.RemoveAll(testDir)
	os.Mkdir(testDir, 0755)
}

func makeKey(k string, v uint16) BtreeKey {
	return BtreeKey{Name: k, Data: IndexData{Offset: v}}
}

// Tests lt of BtreeKey
func TestKeyLt(t *testing.T) {
	// GIVEN
	k1 := makeKey("a", 1)
	k2 := makeKey("b", 2)

	// WHEN
	lt := k1.lt(k2)

	// THEN
	if !lt {
		t.Errorf("k1.lt(k2) = %v, want true", lt)
	}
}

func TestBtreeWhenHasDefaultDegree(t *testing.T) {
	// GIVEN
	r := &BtreeNode{
		Keys: []BtreeKey{makeKey("e", 1), makeKey("k", 2)},
		Children: []*BtreeNode{
			{Keys: []BtreeKey{makeKey("a", 1), makeKey("b", 2), makeKey("c", 3)}, IsLeaf: true, Level: 2},
			{Keys: []BtreeKey{makeKey("fd", 4), makeKey("gd", 5), makeKey("h2", 6)}, IsLeaf: true, Level: 2},
			{Keys: []BtreeKey{makeKey("m1", 7), makeKey("m2", 8), makeKey("root", 9)}, IsLeaf: true, Level: 2}},
		IsLeaf: false,
		Level:  1,
	}
	tree := &Btree{Root: r, degree: 2}

	// WHEN
	tree.Insert(makeKey("food", 10))
	tree.Insert(makeKey("f", 11))
	tree.Insert(makeKey("hi", 12))
	tree.Insert(makeKey("internet", 13))
	tree.Insert(makeKey("j", 14))
	tree.Insert(makeKey("kitty", 15))
	tree.Insert(makeKey("loop", 16))
	tree.Insert(makeKey("m", 17))
	tree.Insert(makeKey("string", 18))

	// THEN
	t.Log("traversing the tree after inserting 9 Keys")
	tree.traverse(tree.Root)

	if len(tree.Root.Keys) != 3 {
		t.Errorf("root should have 3 keys, but got %d", len(tree.Root.Keys))
	}
	if len(tree.Root.Children) != 4 {
		t.Errorf("root should have 4 children, but got %d", len(tree.Root.Children))
	}
	if k := tree.Search("food"); k.Data.Offset != 10 {
		t.Error("food should be found")
	}
	if k := tree.Search("kitty"); k.Data.Offset != 15 {
		t.Error("kitty should be found")
	}
	if k := tree.Search("internet"); k.Data.Offset != 13 {
		t.Error("internet should be found")
	}
	if k := tree.Search("string"); k.Data.Offset != 18 {
		t.Error("string should be found")
	}
	if k := tree.Search("loop"); k.Data.Offset != 16 {
		t.Error("loop should be found")
	}
	if k := tree.Search("hi"); k.Data.Offset != 12 {
		t.Error("hi should be found")
	}
	if k := tree.Search("f"); k.IsEmpty() {
		t.Error("f should not be found")
	}
	if k := tree.Search("z"); !k.IsEmpty() {
		t.Error("z should not be found")
	}
}

func TestBtreeWhenDegreeIs10(t *testing.T) {
	// GIVEN
	r := &BtreeNode{
		Keys:   []BtreeKey{makeKey("e", 1), makeKey("k", 10)},
		IsLeaf: true,
		Level:  1,
	}
	tree := &Btree{Root: r, degree: 5}

	// WHEN
	tree.Insert(makeKey("food", 10))
	tree.Insert(makeKey("f", 11))
	tree.Insert(makeKey("hi", 12))
	tree.Insert(makeKey("internet", 13))
	tree.Insert(makeKey("j", 14))
	tree.Insert(makeKey("kitty", 15))
	tree.Insert(makeKey("loop", 16))

	// THEN
	if len(tree.Root.Keys) != 9 {
		t.Errorf("root should have 9 keys, but got %d", len(tree.Root.Keys))
	}
	if len(tree.Root.Children) != 0 {
		t.Errorf("root should have no children, but got %d", len(tree.Root.Children))
	}
	if k := tree.Search("food"); k.Data.Offset != 10 {
		t.Error("food should be found")
	}
	if k := tree.Search("kitty"); k.Data.Offset != 15 {
		t.Error("kitty should be found")
	}
	if k := tree.Search("internet"); k.Data.Offset != 13 {
		t.Error("internet should be found")
	}
	if k := tree.Search("string"); !k.IsEmpty() {
		t.Error("string should not be found")
	}
}
