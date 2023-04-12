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
		keys: []key{{"e", 0, 0, 0}, {"k", 30, 0, 0}},
		children: []*node{
			{keys: []key{{"a", 1, 0, 0}, {"b", 2, 0, 0}, {"v", 3, 0, 0}}, isLeaf: true, level: 2},
			{keys: []key{{"fd", 4, 0, 0}, {"gd", 5, 0, 0}, {"h2", 6, 0, 0}}, isLeaf: true, level: 2},
			{keys: []key{{"m1", 7, 0, 0}, {"m2", 8, 0, 0}, {"root", 9, 0, 0}}, isLeaf: true, level: 2}},
		isLeaf: false,
		level:  1,
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
	t.Log("traversing the tree after inserting food, godd, hi, internet, j, kitty, loop, moon, string")
	traverse(root)

	if len(root.keys) != 3 {
		t.Errorf("root should have 3 keys, but got %d", len(root.keys))
	}
	if len(root.children) != 4 {
		t.Errorf("root should have 4 children, but got %d", len(root.children))
	}
	if v := root.search("food"); v != 10 {
		t.Error("food should be found")
	}
	if v := root.search("kitty"); v != 15 {
		t.Error("kitty should be found")
	}
	if v := root.search("internet"); v != 13 {
		t.Error("internet should be found")
	}
	if v := root.search("string"); v != 18 {
		t.Error("string should be found")
	}
	if v := root.search("loop"); v != 16 {
		t.Error("loop should be found")
	}
	if v := root.search("hi"); v != 12 {
		t.Error("hi should be found")
	}
	if v := root.search("f"); v != -1 {
		t.Error("f should not be found")
	}
	if v := root.search("z"); v != -1 {
		t.Error("z should not be found")
	}
}
