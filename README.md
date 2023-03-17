# gpostgres
An implementation of Postgresql with Golang.

## Building steps
### Section 1

In this section, our goal is that can just create table with `TEXT` and `INT` column type in memory, and when execuate statement like `CREATE TABLE user (name TEXT, age INT);`, we can get the result in a simple REPL.

To achieve this goal, we can split it into several steps below.

1. A simple REPL;
2. Create table without columns;
3. Create table with `TEXT` columns;
4. Create table with `TEXT` and `INT` column types;
5. Return table scheme when run `\d table`

### Section 2

In this section, our goal is that can run `INSERT`, `SELETE`, `UPDATE` and `DELETE` statements like `CREATE`. And in this stage, all our data is in memory.

### Section 3

In this section, we will support data persistence in local file system in some structures.

### Section 4

In this section, we will suppport C/S architecture. That is, we can do data manipulation from TCP connect in any PL context.
