package storage

type Table struct {
	Name string
	Len  uint8
	Rows []string
}

func NewTable(name string) *Table {
	return &Table{
		Name: name,
	}
}
