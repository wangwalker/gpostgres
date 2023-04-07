package storage

import "testing"

func TestIntBtree(t *testing.T) {
	// GIVEN
	root := &node[int]{
		keys: []int{50, 100},
		children: []*node[int]{
			{keys: []int{10, 20, 30}, isLeaf: true, level: 2},
			{keys: []int{60, 70, 80}, isLeaf: true, level: 2},
			{keys: []int{110, 120, 130}, isLeaf: true, level: 2}},
		isLeaf: false,
		level:  1,
	}
	t.Log("traversing the tree before insertion")
	traverse(root)

	// WHEN
	root.insert(38)
	root.insert(75)
	root.insert(23)
	root.insert(123)
	root.insert(90)
	root.insert(240)
	root.insert(14)
	root.insert(5)
	root.insert(200)

	// THEN
	t.Log("traversing the tree after inserting 38, 75, 23, 123, 90, 240, 14, 5, 200")
	traverse(root)

	if len(root.keys) != 3 {
		t.Errorf("root should have 3 keys, but got %d", len(root.keys))
	}
	if len(root.children) != 4 {
		t.Errorf("root should have 4 children, but got %d", len(root.children))
	}
	if c := root.search(5); c == nil {
		t.Error("5 should be found")
	}
	if c := root.search(240); c == nil {
		t.Error("240 should be found")
	}
	if c := root.search(123); c == nil {
		t.Error("123 should be found")
	}
	if c := root.search(200); c == nil {
		t.Error("200 should be found")
	}
	if c := root.search(14); c == nil {
		t.Error("14 should be found")
	}
	if c := root.search(23); c == nil {
		t.Error("23 should be found")
	}
	if c := root.search(39); c != nil {
		t.Error("39 should not be found")
	}
}

func TestStringBtree(t *testing.T) {
	// GIVEN
	root := &node[string]{
		keys: []string{"e", "k"},
		children: []*node[string]{
			{keys: []string{"a", "b", "c"}, isLeaf: true, level: 2},
			{keys: []string{"fd", "gd", "h2"}, isLeaf: true, level: 2},
			{keys: []string{"m1", "m2", "root"}, isLeaf: true, level: 2}},
		isLeaf: false,
		level:  1,
	}
	t.Log("traversing the tree before insertion")
	traverse(root)

	// WHEN
	root.insert("food")
	root.insert("godd")
	root.insert("hi")
	root.insert("internet")
	root.insert("j")
	root.insert("kitty")
	root.insert("loop")
	root.insert("moon")
	root.insert("string")

	// THEN
	t.Log("traversing the tree after inserting food, godd, hi, internet, j, kitty, loop, moon, string")
	traverse(root)

	if len(root.keys) != 3 {
		t.Errorf("root should have 3 keys, but got %d", len(root.keys))
	}
	if len(root.children) != 4 {
		t.Errorf("root should have 4 children, but got %d", len(root.children))
	}
	if c := root.search("food"); c == nil {
		t.Error("food should be found")
	}
	if c := root.search("kitty"); c == nil {
		t.Error("kitty should be found")
	}
	if c := root.search("internet"); c == nil {
		t.Error("internet should be found")
	}
	if c := root.search("string"); c == nil {
		t.Error("string should be found")
	}
	if c := root.search("loop"); c == nil {
		t.Error("loop should be found")
	}
	if c := root.search("hi"); c == nil {
		t.Error("hi should be found")
	}
	if c := root.search("f"); c != nil {
		t.Error("f should not be found")
	}
	if c := root.search("z"); c != nil {
		t.Error("z should not be found")
	}
}
