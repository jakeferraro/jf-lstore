# L-Store Database

A column-oriented database with 2PL, transactions, and disk persistence.

## Installation

```bash
pip install -r requirements.txt
```

## Features

### Milestone 1: Core Database Operations
- B-tree indexing (using sortedcontainers, primary + secondary indexes)
- Page range architecture (16 base pages per range, 512 records/page)
- Non-cumulative tail records for updates
- CRUD operations: insert, select, update, delete, sum

### Milestone 2: Durability
- Database persistence (open/close)
- Table metadata and data serialization
- Disk-based page storage

### Milestone 3: Concurrency & Performance
- ACID transactions with 2PL (two-phase locking)
- Shared/exclusive record-level locks
- LRU bufferpool with configurable page limit
- Transaction worker threads for concurrent execution

## Performance

- Insert/Select/Update/Delete: O(log n)
- Range sum aggregation: O(log n + k) where k = records in range
- Lock acquisition: optimistic with retry/abort on conflict

## Dependencies

- `sortedcontainers==2.4.0` - B-tree index implementation
