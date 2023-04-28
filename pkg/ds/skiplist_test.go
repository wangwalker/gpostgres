package ds

import "testing"

type mockDicisionMaker struct {
	shouldInsert bool
}

func (m *mockDicisionMaker) ShouldInsert() bool {
	return m.shouldInsert
}

func makeIndexData(o uint16) IndexData {
	return IndexData{o, 0, 0, 0}
}

func TestCreateSkipListNode(t *testing.T) {
	data := IndexData{1, 1, 0, 0}
	head := NewSkipList("a", data)
	if head.Key != "a" || head.Data.Offset != 1 || head.Right != nil || head.Down != nil {
		t.Errorf("NewSkipList(\"a\", 1) = %v, want \"a\"", head)
	}
}

func TestInsertAndSearchSkipListNode(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", makeIndexData(1))
	head = head.Insert("b", makeIndexData(2))
	head = head.Insert("c", makeIndexData(3))
	head = head.Insert("d", makeIndexData(4))
	head = head.Insert("e", makeIndexData(5))

	// WHEN
	r1 := head.Search("a").Offset
	r2 := head.Search("b").Offset
	r3 := head.Search("c").Offset
	r4 := head.Search("d").Offset
	r5 := head.Search("e").Offset

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
	head := NewSkipList("a", makeIndexData(1))
	head.SetDicisionMaker(&mockDicisionMaker{true})
	head = head.Insert("b", makeIndexData(2))
	head = head.Insert("c", makeIndexData(3))

	// WHEN
	head.Update("b", makeIndexData(4))

	// THEN
	if head.Search("b").Offset != 4 {
		t.Errorf("head.Search(b) = %v, want \"4\"", head.Search("b"))
	}
}

func TestUpdateSkipListHeadNode(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", makeIndexData(1))
	head = head.Insert("b", makeIndexData(12))
	head = head.Insert("c", makeIndexData(13))
	head = head.Insert("d", makeIndexData(14))

	// WHEN
	head.Update("a", makeIndexData(11))

	// THEN
	if head.Search("a").Offset != 11 {
		t.Errorf("head.Search(a) = %v, want \"11\"", head.Search("a"))
	}
	for p := head; p != nil; p = p.Down {
		if p.Key == "a" && p.Data.Offset != 11 {
			t.Errorf("p.value = %v, want \"11\"", p.Data.Offset)
		}
	}

}

func TestInsertWithMockDicisionMaker(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", makeIndexData(1))
	head.SetDicisionMaker(&mockDicisionMaker{false})
	head = head.Insert("b", makeIndexData(2))

	// GIVEN
	head.SetDicisionMaker(&mockDicisionMaker{true})
	head = head.Insert("c", makeIndexData(3))

	// THEN
	if head.Key != "a" {
		t.Errorf("head.Key = %v, want 1", head.Key)
	}
	if head.Right.Key != "c" {
		t.Errorf("head.Right.Key = %v, want c", head.Right.Key)
	}
	if head.Down.Key != "a" {
		t.Errorf("head.Down.Key = %v, want a", head.Down.Key)
	}
	if head.Down.Right.Key != "b" || head.Down.Right.Right.Key != "c" {
		t.Errorf("head.Down.Right.Key = %v, want b", head.Down.Right.Key)
	}

	// GIVEN
	head.SetDicisionMaker(&mockDicisionMaker{false})
	head = head.Insert("d", makeIndexData(4))
	head.SetDicisionMaker(&mockDicisionMaker{true})
	head = head.Insert("e", makeIndexData(5))

	// THEN
	if head.Key != "a" {
		t.Errorf("head.Key = %v, want a", head.Key)
	}
	if head.Right.Key != "e" {
		t.Errorf("head.Right.Key = %v, want e", head.Right.Key)
	}
	if head.Down.Key != "a" {
		t.Errorf("head.Down.Key = %v, want a", head.Down.Key)
	}
	if head.Down.Right.Key != "c" || head.Down.Right.Right.Key != "e" {
		t.Errorf("head.Down.Right.Key = %v, want c", head.Down.Right.Key)
		t.Errorf("head.Down.Right.Right.Key = %v, want e", head.Down.Right.Right.Key)
	}
	if head.Down.Down.Key != "a" {
		t.Errorf("head.Down.Down.Key = %v, want a", head.Down.Down.Key)
	}
	if head.Down.Down.Right.Right.Right.Key != "d" {
		t.Errorf("head.Down.Down.Right.Right.Right.Key = %v, want 4", head.Down.Down.Right.Right.Right.Key)
	}
}

func TestDeleteSkipListNode(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", makeIndexData(1))
	head = head.Insert("b", makeIndexData(2))
	head = head.Insert("c", makeIndexData(3))
	head = head.Insert("d", makeIndexData(4))
	head = head.Insert("e", makeIndexData(5))

	// WHEN
	head.Delete("c")

	// THEN
	if head.Search("c").Offset != 0 {
		t.Errorf("head.Search(3) = %v, want 0", head.Search("c"))
	}
	if head.Down != nil && head.Down.Search("c").Offset != 0 {
		t.Errorf("head.Down.Search(3) = %v, want 0", head.Down.Search("c"))
	}
	if head.Right != nil && head.Right.Search("c").Offset != 0 {
		t.Errorf("head.Right.Search(3) = %v, want 0", head.Right.Search("c"))
	}
	if head.Right.Down != nil && head.Right.Down.Search("c").Offset != 0 {
		t.Errorf("head.Right.Down.Search(3) = %v, want 0", head.Right.Down.Search("c"))
	}
}

func TestTheMostLowListContainsAllNodes(t *testing.T) {
	// GIVEN
	head := NewSkipList("a", makeIndexData(1))
	head.SetDicisionMaker(&mockDicisionMaker{true})
	head = head.Insert("b", makeIndexData(2))
	head = head.Insert("c", makeIndexData(3))
	head = head.Insert("d", makeIndexData(4))
	head = head.Insert("e", makeIndexData(5))

	// WHEN
	nodes := head.AllNodes()

	// THEN
	if len(nodes) != 5 {
		t.Errorf("len(nodes) = %v, want 5", len(nodes))
	}
	if nodes[0].Key != "a" && nodes[0].Data.Offset != 1 {
		t.Errorf("nodes[0] = %v, want a", nodes[0])
	}
	if nodes[1].Key != "b" && nodes[1].Data.Offset != 2 {
		t.Errorf("nodes[1] = %v, want b", nodes[1])
	}
	if nodes[2].Key != "c" && nodes[2].Data.Offset != 3 {
		t.Errorf("nodes[2] = %v, want c", nodes[2])
	}
	if nodes[3].Key != "d" && nodes[3].Data.Offset != 4 {
		t.Errorf("nodes[3] = %v, want d", nodes[3])
	}
	if nodes[4].Key != "e" && nodes[4].Data.Offset != 5 {
		t.Errorf("nodes[4] = %v, want e", nodes[4])
	}

}
