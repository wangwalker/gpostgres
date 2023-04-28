package ds

import "testing"

type mockDicisionMaker struct {
	shouldInsert bool
}

func (m *mockDicisionMaker) ShouldInsert() bool {
	return m.shouldInsert
}

func TestCreateSkipListNode(t *testing.T) {
	head := NewSkipList("a", 1)
	if head.key != "a" || head.value != 1 || head.right != nil || head.down != nil {
		t.Errorf("NewSkipList(1, \"a\") = %v, want %v", head, SkipListNode{"a", 1, nil, nil, &RandomDicisionMaker{}})
	}
}

func TestInsertAndSearchSkipListNode(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", 1)
	head = head.Insert("b", 2)
	head = head.Insert("c", 3)
	head = head.Insert("d", 4)
	head = head.Insert("e", 5)

	// WHEN
	r1 := head.Search("a")
	r2 := head.Search("b")
	r3 := head.Search("c")
	r4 := head.Search("d")
	r5 := head.Search("e")

	// THEN
	if r1 != 1 {
		t.Errorf("head.Search(1) = %v, want \"1\"", r1)
	}
	if r2 != 2 {
		t.Errorf("head.Search(2) = %v, want \"2\"", r2)
	}
	if r3 != 3 {
		t.Errorf("head.Search(3) = %v, want \"3\"", r3)
	}
	if r4 != 4 {
		t.Errorf("head.Search(4) = %v, want \"4\"", r4)
	}
	if r5 != 5 {
		t.Errorf("head.Search(5) = %v, want \"5\"", r5)
	}
}

func TestUpdateSkipListNode(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", 1)
	head = head.Insert("b", 2)
	head = head.Insert("c", 3)

	// WHEN
	head.Update("b", 4)

	// THEN
	if head.Search("b") != 4 {
		t.Errorf("head.Search(2) = %v, want \"4\"", head.Search("b"))
	}
}

func TestUpdateSkipListHeadNode(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", 1)
	head = head.Insert("b", 12)
	head = head.Insert("c", 13)
	head = head.Insert("d", 14)

	// WHEN
	head.Update("a", 11)

	// THEN
	if head.Search("a") != 11 {
		t.Errorf("head.Search(a) = %v, want \"11\"", head.Search("a"))
	}
	for p := head; p != nil; p = p.down {
		if p.key == "a" && p.value != 11 {
			t.Errorf("p.value = %v, want \"11\"", p.value)
		}
	}

}

func TestInsertWithMockDicisionMaker(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", 1)
	head.SetDicisionMaker(&mockDicisionMaker{false})
	head = head.Insert("b", 2)

	// GIVEN
	head.SetDicisionMaker(&mockDicisionMaker{true})
	head = head.Insert("c", 3)

	// THEN
	if head.key != "a" {
		t.Errorf("head.key = %v, want 1", head.key)
	}
	if head.right.key != "c" {
		t.Errorf("head.right.key = %v, want c", head.right.key)
	}
	if head.down.key != "a" {
		t.Errorf("head.down.key = %v, want a", head.down.key)
	}
	if head.down.right.key != "b" || head.down.right.right.key != "c" {
		t.Errorf("head.down.right.key = %v, want b", head.down.right.key)
	}

	// GIVEN
	head.SetDicisionMaker(&mockDicisionMaker{false})
	head = head.Insert("d", 4)
	head.SetDicisionMaker(&mockDicisionMaker{true})
	head = head.Insert("e", 5)

	// THEN
	if head.key != "a" {
		t.Errorf("head.key = %v, want a", head.key)
	}
	if head.right.key != "e" {
		t.Errorf("head.right.key = %v, want e", head.right.key)
	}
	if head.down.key != "a" {
		t.Errorf("head.down.key = %v, want a", head.down.key)
	}
	if head.down.right.key != "c" || head.down.right.right.key != "e" {
		t.Errorf("head.down.right.key = %v, want c", head.down.right.key)
		t.Errorf("head.down.right.right.key = %v, want e", head.down.right.right.key)
	}
	if head.down.down.key != "a" {
		t.Errorf("head.down.down.key = %v, want a", head.down.down.key)
	}
	if head.down.down.right.right.right.key != "d" {
		t.Errorf("head.down.down.right.right.right.key = %v, want 4", head.down.down.right.right.right.key)
	}
}

func TestDeleteSkipListNode(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", 1)
	head = head.Insert("b", 2)
	head = head.Insert("c", 3)
	head = head.Insert("d", 4)
	head = head.Insert("e", 5)

	// WHEN
	head.Delete("c")

	// THEN
	if head.Search("c") != 0 {
		t.Errorf("head.Search(3) = %v, want 0", head.Search("c"))
	}
	if head.down.Search("c") != 0 {
		t.Errorf("head.down.Search(3) = %v, want 0", head.down.Search("c"))
	}
	if head.right.Search("c") != 0 {
		t.Errorf("head.right.Search(3) = %v, want 0", head.right.Search("c"))
	}
	if head.right.down.Search("c") != 0 {
		t.Errorf("head.right.down.Search(3) = %v, want 0", head.right.down.Search("c"))
	}
}

func TestTheMostLowListContainsAllNodes(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", 1)
	head.SetDicisionMaker(&mockDicisionMaker{true})
	head = head.Insert("b", 2)
	head = head.Insert("c", 3)
	head = head.Insert("d", 4)
	head = head.Insert("e", 5)

	// WHEN
	p := head
	for p.down != nil {
		p = p.down
	}
	data := make(map[string]int)
	for p != nil {
		data[p.key] = int(p.value)
		p = p.right
	}

	// THEN
	if len(data) != 5 {
		t.Errorf("len(data) = %v, want 5", len(data))
	}
	if data["a"] != 1 {
		t.Errorf("data[\"a\"] = %v, want 1", data["a"])
	}
	if data["b"] != 2 {
		t.Errorf("data[\"b\"] = %v, want 2", data["b"])
	}
	if data["c"] != 3 {
		t.Errorf("data[\"c\"] = %v, want 3", data["c"])
	}
	if data["d"] != 4 {
		t.Errorf("data[\"d\"] = %v, want 4", data["d"])
	}
	if data["e"] != 5 {
		t.Errorf("data[\"e\"] = %v, want 5", data["e"])
	}

}
