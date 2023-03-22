# gpostgres
An implementation of Postgresql with Golang.

## Building steps
### Section 1

In this section, our goal is that can just create table with `TEXT` and `INT` column type in memory, and when execuate statement like `CREATE TABLE user (name TEXT, age INT);`, we can get the result in a simple REPL.

To achieve this goal, we can split it into several steps below.

- [x] A simple REPL
- [x] Create table without columns
- [x] Create table with `TEXT` and `INT` column types
- [x] Return tables scheme when run`\d` or `\d table`

```bash
postgres# \h
help
postgres# create table users (name text, age int);
create table: users OK!
postgres# \d
List of relations
users
postgres# \d users
Column     | Type                |
---------- + --------------------|
name       | Text                |
age        | Int                 |

postgres# create table users (nate text);
Error: invalid query create table users (nate text);, error: table already existed
postgres# \q  
quit
```
### Section 2

In this section, our goal is that can run `INSERT`, `SELETE`, `UPDATE` and `DELETE` statements like `CREATE`. And in this stage, all our data is in memory.

### Section 3

In this section, we will support data persistence in local file system in some structures.

### Section 4

In this section, we will suppport C/S architecture. That is, we can do data manipulation from TCP connect in any PL context.
