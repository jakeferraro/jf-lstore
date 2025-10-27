# L-Store Database

## Installation and Setup

### Prerequisites
- Python 3.7 or higher

### Installing Dependencies

This project requires the `sortedcontainers` library for B-tree index implementation. To install all dependencies:

```bash
pip install -r requirements.txt
```

Or install manually:
```bash
pip install sortedcontainers==2.4.0
```

## Running Tests

### Main Performance Test
```bash
python3 __main__.py
```
or
```bash
python __main__.py
```

### Milestone 1 Tester
```bash
python3 m1_tester.py
```

### Expected Output
All tests should pass without errors. The main test will output timing statistics for:
- Insert operations
- Update operations
- Select operations
- Aggregation operations
- Delete operations


## Implementation Highlights

### 1. B-Tree Indexes
- Primary key is automatically indexed using a B-tree (via sortedcontainers.SortedDict)
- O(log n) lookup performance for select, update, and delete operations
- Range query support for aggregation operations
- Secondary indexes supported via `create_index()` method

### 2. Page Range Architecture
- Records organized into page ranges (16 base pages per range, 512 records per page)
- Automatic overflow to new page ranges when capacity reached
- Each page range has its own tail pages for updates

### 3. Tail Record Management
- Non-cumulative tail records (only stores updated columns)
- Proper tail record invalidation on delete operations
- Schema encoding tracks which columns have been updated

## Dependencies

### sortedcontainers
This project uses the `sortedcontainers` library to implement B-tree indices. This is a pure-Python library that provides:
- SortedDict for ordered key-value storage
- O(log n) insertion, deletion, and lookup
- Efficient range queries

**Alternative**: If for any reason `sortedcontainers` cannot be used, the code could be modified to use Python's built-in `dict` (with O(1) lookup but no range query support) or a custom B-tree implementation.

## Performance Characteristics

With B-tree indexing enabled:
- Insert: O(log n) per record
- Select by primary key: O(log n)
- Update by primary key: O(log n)
- Delete by primary key: O(log n)
- Range sum aggregation: O(log n + k) where k is the number of records in range