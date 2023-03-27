package lexer

import (
	"testing"

	"github.com/wangwalker/gpostgres/pkg/ast"
	"github.com/wangwalker/gpostgres/pkg/storage"
)

func TestCreateTableFailed(t *testing.T) {
	createTests := []struct {
		source string
	}{
		{source: "create table users"},
		{source: "create table users from"},
		{source: "create table users ((name text)"},
		{source: "create table users (name text))"},
		{source: "create table users ((name text))"},
		{source: "create table users (text name);"},
		{source: "create table users1 user2 (name text)"},
		{source: "create table users2 (name text, text, age int);"},
		{source: "create table users2 (name text, age int, gender);"},
	}

	for i, tt := range createTests {
		_, err := Lex(tt.source)
		if err == nil {
			t.Errorf("%s: test %d should create table failed, but err is null", t.Name(), i)
		}
	}
}

func TestCreateTableSucceed(t *testing.T) {
	createTests := []struct {
		source  string
		stmt    ast.QueryStmtCreateTable
		columns int
	}{
		{source: "create table users1 (name text)", stmt: ast.QueryStmtCreateTable{Name: "users1"}, columns: 1},
		{source: "create table users2 (name int);", stmt: ast.QueryStmtCreateTable{Name: "users2"}, columns: 1},
		{source: "create table users3 ( name text );", stmt: ast.QueryStmtCreateTable{Name: "users3"}, columns: 1},
		{source: "create table cities (a text, b text);", stmt: ast.QueryStmtCreateTable{Name: "cities"}, columns: 2},
		{source: "create table cities2 (a text, b int, c text);", stmt: ast.QueryStmtCreateTable{Name: "cities2"}, columns: 3},
	}

	for i, tt := range createTests {
		stmt, err := Lex(tt.source)
		if err != nil {
			t.Errorf("%s: test %d should create table ok, but err is not null: %v", t.Name(), i, err)
		}
		create, ok := stmt.(*ast.QueryStmtCreateTable)
		if !ok {
			t.Errorf("%s: test %d should create table ok, but return wrong type", t.Name(), i)
		}
		if tt.stmt.Name != create.Name {
			t.Errorf("%s: test %d should create table ok, but name is not equal", t.Name(), i)
		}
		if tt.columns != len(create.Columns) {
			t.Errorf("%s: test %d should create table ok, but get wrong columns", t.Name(), i)
		}
	}
}

func TestInsertFailsWhenTableNotExist(t *testing.T) {
	insertTests := []struct {
		source string
	}{
		{source: "insert"},
		{source: "update"},
		{source: "insert table"},
		{source: "insert into users"},
		{source: "insert into users values"},
		{source: "insert into users (name text)"},
	}
	for i, tt := range insertTests {
		_, err := Lex(tt.source)
		if err == nil {
			t.Errorf("%s: test %d should fail, but err is null", t.Name(), i)
		}
	}
}

func TestInsertFaillsWhenSyntaxWrong(t *testing.T) {
	// Create a table named users
	_, err := Lex("create table u (name text, age int);")
	if err != nil {
		t.Errorf("%s: should create table ok, but err: %v", t.Name(), err)
	}

	insertTests := []string{
		"insert into us ('a', 11)",
		"create into u values ('a', 11)",
		"insert into u values ('a')",
		"insert into u values ('a', 'b', 'c');",
		"insert into u values )'a', 11(",
		"insert into u (n, a) values ()",
		"insert into u (n, a) values ('a', 2)",
		"insert into u (name, a) values ('a', 2)",
		"insert into u (name, age, gender) values ('a', 2)",
		"insert into u (name, age) values ('w', 2, 3)",
	}
	for i, tt := range insertTests {
		_, err := Lex(tt)
		if err == nil {
			t.Errorf("%s: test %d should fail, but error is null", t.Name(), i)
		}
	}
}

func TestInsertSucceed(t *testing.T) {
	// Create a table named users
	_, err := Lex("create table users (name text, age int);")
	if err != nil {
		t.Errorf("%s: should create table ok, but err: %v", t.Name(), err)
	}

	insertTests := []string{
		"insert into users values ('a', 11)",
		"insert into users (name, age) values ('a', 11)",
		"insert into users values ('a', 11), ('b', 12);",
		"insert into users (name, age) values ('a', 11), ('b', 12);",
	}
	for i, tt := range insertTests {
		_, err := Lex(tt)
		if err != nil {
			t.Errorf("%s: test %d should succeed, but error is not null: %v", t.Name(), i, err)
		}
	}
}

func TestSelectFailsWhenTableNotExist(t *testing.T) {
	selectTests := []string{
		"select from;",
		"select * from;",
		"select * from nonexistedtable;",
		"select a, b from c;",
		"select () from c",
	}
	for i, tt := range selectTests {
		_, err := Lex(tt)
		if err == nil {
			t.Errorf("%s: test %d should fail, but err is null", t.Name(), i)
		}
	}
}

func TestSelectFailsWhenTableExistButWrongColumns(t *testing.T) {
	// GIVEN
	createAndInsert := []string{
		"create table stu (name text, age int);",
		"insert into stu values ('a', 11)",
		"insert into stu (name, age) values ('a', 12)",
		"insert into stu values ('a', 13), ('b', 12);",
		"insert into stu (name, age) values ('a', 14), ('b', 12);",
	}
	for i, tt := range createAndInsert {
		_, err := Lex(tt)
		if err != nil {
			t.Errorf("%s: given: test %d should ok, but err isn't null", t.Name(), i)
		}
	}

	// WHEN
	selectTests := []string{
		"select (a) from stu where name == 'a';",
		"select (a, b) from stu where name == 'a';",
		"select a, b from stu where name == 'a';",
		"select (a, b from stu where name == 'a';",
		"select (a, b) from stu;",
		"select (name, b) from stu where name == 'a';",
		"select (name, age, c) from stu where name == 'a';",
	}
	// THEN
	for i, tt := range selectTests {
		_, err := Lex(tt)
		if err == nil {
			t.Errorf("%s: then: %d should fail, but err is null", t.Name(), i)
		}
	}
}

func TestSelectFailsWhenTableExistButWrongWhere(t *testing.T) {
	// GIVEN
	createAndInsert := []string{
		"create table stu1 (name text, age int);",
		"insert into stu1 values ('a', 11)",
		"insert into stu1 (name, age) values ('a', 12)",
		"insert into stu1 values ('a', 13), ('b', 12);",
		"insert into stu1 (name, age) values ('a', 14), ('b', 12);",
	}
	for i, tt := range createAndInsert {
		_, err := Lex(tt)
		if err != nil {
			t.Errorf("%s: given: test %d should ok, but err isn't null", t.Name(), i)
		}
	}

	// WHEN
	selectTests := []string{
		// "select * from stu1 == 'a';",
		"select * from stu1 nn == 'a';",
		"select * from stu1 where == 'a';",
		"select * from stu1 where name = 'a';",
		"select (name, age) from stu1 where name === 'a';",
	}
	// THEN
	for i, tt := range selectTests {
		_, err := Lex(tt)
		if err == nil {
			t.Errorf("%s: then: %d should fail, but err is null", t.Name(), i)
		}
	}
}

func TestSelectSucceed(t *testing.T) {
	// GIVEN
	createAndInsert := []string{
		"create table stu2 (name text, age int);",
		"insert into stu2 values ('a', 11)",
		"insert into stu2 (name, age) values ('a', 12)",
		"insert into stu2 values ('a', 13), ('b', 22);",
		"insert into stu2 (name, age) values ('a', 14), ('b', 12);",
	}
	for i, tt := range createAndInsert {
		_, err := Lex(tt)
		if err != nil {
			t.Errorf("%s: given: test %d should ok, but err isn't null", t.Name(), i)
		}
	}

	// WHEN
	selectTests := []struct {
		source string
		rows   int
	}{
		{"select * from stu2;", 6},
		{"select (name) from stu2;", 6},
		{"select (name, age) from stu2;", 6},
		{"select * from stu2 where name == 'a';", 4},
		{"select * from stu2 where name != 'a';", 2},
		{"select (name) from stu2 where name == 'a';", 4},
		{"select (name, age) from stu2 where name != 'a'", 2},
	}
	// THEN
	for i, tt := range selectTests {
		r, err := Lex(tt.source)
		if err != nil {
			t.Errorf("%s: then: test %d should ok, but err isn't null", t.Name(), i)
		}
		rows, ok := r.([]storage.Row)
		if !ok || len(rows) != tt.rows {
			t.Errorf("%s: then: test %d should get %d rows, but got %d ", t.Name(), i, tt.rows, len(rows))
		}
	}
}
