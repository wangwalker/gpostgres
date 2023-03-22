package ast

type QueryStmtKind uint

const (
	QueryStmtKindCreate = iota
	QueryStmtKindEmpty
	QueryStmtKindUnkown
)

type ColumnName string
type ColumnKind uint8

func (c ColumnKind) String() string {
	switch c {
	case ColumnKindInt:
		return "Int"
	case ColumnKindText:
		return "Text"
	}
	return ""
}

type Column struct {
	Name ColumnName
	Kind ColumnKind
}

const (
	ColumnKindText ColumnKind = iota + 1
	ColumnKindInt
	ColumnKindUnknown
)

type QueryStmtCreateTable struct {
	Name    string
	Columns []Column
}
