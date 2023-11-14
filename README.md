# Rust sqlite clone

A simple clone of sqlite.

## How to use

### TCP

TCP is the default mode,
the host and port are configured with
--port and --host flags

make POST request that contain SQL query to the server with Content-Type of text/plain

```http
POST /
Content-Type: text/plain

CREATE TABLE info (id uint);

```

### Repl

To run in repl mode run with the --repel flag.

```
exe --repel
```

## Supported queries

1. INSERT INTO table VALUES (value,value,...);
1. INSERT INTO table (column,column,...) VALUES (value,value,...);
1. CREATE TABLE table (column data_type);
1. SELECT \* FROM table;
1. SELECT (column, column,...) FROM table;
1. DELETE FROM table WHERE expr;
1. DROP TABLE table;
1. UPDATE table SET column=expr WHERE expr;

## Supported Data types

1. string
1. uint
1. u64
1. null
