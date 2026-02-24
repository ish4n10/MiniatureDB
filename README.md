# db-engine (storage engine)


## Under Development!


MiniatureDB is a simple storage engien implementation of a database in C++ inspired from SQLite and InnoDB.

Summary of what this implementation resembles
- SQLite (storage layer): an embedded, single-file storage engine.
- InnoDB (conceptual inspiration): uses a page-based buffer pool, page-level metadata (parent/child links, LSN field present in page headers), and B+Tree internals with split/merge logic — similar concepts to InnoDB's page layout and caching strategies (but far simpler and missing many production features).

Notes and limitations
- This engine is a simplified educational implementation. It does not provide a complete ACID transaction subsystem, full crash recovery (WAL), concurrency control (MVCC/locks), or advanced storage optimizations present in production systems like InnoDB or SQLite.
