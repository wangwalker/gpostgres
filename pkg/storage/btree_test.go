package storage

import "testing"

// Tests lt of key
func TestKeyLt(t *testing.T) {
	// GIVEN
	k1 := key{"a", 1, 0, 0}
	k2 := key{"b", 2, 0, 0}

	// WHEN
	lt := k1.lt(k2)

	// THEN
	if !lt {
		t.Errorf("k1.lt(k2) = %v, want true", lt)
	}
}

func TestStringBtree(t *testing.T) {
	// GIVEN
	root := &node{
		Keys: []key{{"e", 0, 0, 0}, {"k", 30, 0, 0}},
		Children: []*node{
			{Keys: []key{{"a", 1, 0, 0}, {"b", 2, 0, 0}, {"k", 3, 0, 0}}, IsLeaf: true, Level: 2},
			{Keys: []key{{"fd", 4, 0, 0}, {"gd", 5, 0, 0}, {"h2", 6, 0, 0}}, IsLeaf: true, Level: 2},
			{Keys: []key{{"m1", 7, 0, 0}, {"m2", 8, 0, 0}, {"root", 9, 0, 0}}, IsLeaf: true, Level: 2}},
		IsLeaf: false,
		Level:  1,
	}
	t.Log("traversing the tree before insertion")
	traverse(root)

	// WHEN
	root.insert(key{"food", 10, 0, 0})
	root.insert(key{"godd", 11, 0, 0})
	root.insert(key{"hi", 12, 0, 0})
	root.insert(key{"internet", 13, 0, 0})
	root.insert(key{"j", 14, 0, 0})
	root.insert(key{"kitty", 15, 0, 0})
	root.insert(key{"loop", 16, 0, 0})
	root.insert(key{"moon", 17, 0, 0})
	root.insert(key{"string", 18, 0, 0})

	// THEN
	t.Log("traversing the tree after inserting 9 Keys")
	traverse(root)

	if len(root.Keys) != 3 {
		t.Errorf("root should have 3 Keys, but got %d", len(root.Keys))
	}
	if len(root.Children) != 4 {
		t.Errorf("root should have 4 Children, but got %d", len(root.Children))
	}
	if k := root.search("food"); k.Value != 10 {
		t.Error("food should be found")
	}
	if k := root.search("kitty"); k.Value != 15 {
		t.Error("kitty should be found")
	}
	if k := root.search("internet"); k.Value != 13 {
		t.Error("internet should be found")
	}
	if k := root.search("string"); k.Value != 18 {
		t.Error("string should be found")
	}
	if k := root.search("loop"); k.Value != 16 {
		t.Error("loop should be found")
	}
	if k := root.search("hi"); k.Value != 12 {
		t.Error("hi should be found")
	}
	if k := root.search("f"); !k.isEmpty() {
		t.Error("f should not be found")
	}
	if k := root.search("z"); !k.isEmpty() {
		t.Error("z should not be found")
	}
}
