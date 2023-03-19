package ast

type QueryStmtKind uint

const (
	QueryStmtKindCreate = iota
	QueryStmtKindEmpty
	QueryStmtKindUnkown
)

type ColumnValue string
type ColumnKind uint8

type Column struct {
	Value ColumnValue
	Kind  ColumnKind
}

const (
	ColumnKindText ColumnKind = iota
	ColumnKindInt
)

type QueryStmtCreateTable struct {
	Name    string
	Columns []Column
}
