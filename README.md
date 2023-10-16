# Rust sqlite clone

A simple clone of sqlite.

## How to use

### TCP

TCP is the default mode,
the host and port are configured with
--port and --host flags

make POST request to the server with Content-Type of text/plain

```http
POST /
Content-Type: text/plain

CREATE TABLE info (id uint);

```

### Repl

To use the sqlite system in repl mode run

```

rust_db --repel

```

This with start the server in repl mode.
