from lstore.db import Database
from lstore.query import Query

# Create database and table
db = Database()
grades_table = db.create_table('Grades', 5, 0)  # 5 columns, column 0 is key
query = Query(grades_table)

print("=== Testing Update Function ===\n")

# Test 1: Insert a record
print("Test 1: Insert initial record")
key = 906659671
query.insert(key, 93, 85, 90, 88)
print(f"Inserted record with key={key}, grades=[93, 85, 90, 88]")

# Verify insert
records = query.select(key, 0, [1, 1, 1, 1, 1])
print(f"Select after insert: {[records[0].columns if records else 'No records']}")
print()

# Test 2: Update one column
print("Test 2: Update column 1 (grade from 93 to 95)")
result = query.update(key, None, 95, None, None, None)
print(f"Update returned: {result}")

# Check tail pages were created
print(f"Number of tail page columns: {len(grades_table.tail_pages)}")
print(f"Number of records in first tail page: {grades_table.tail_pages[0][0].num_records}")
print(f"Next tail position: {grades_table.next_tail_position}")

# Check base record metadata
base_rid = 0
base_page_idx, base_slot = grades_table.page_directory[base_rid]
base_indirection = grades_table.base_pages[0][base_page_idx].read(base_slot)
base_schema = grades_table.base_pages[3][base_page_idx].read(base_slot)
print(f"Base record indirection: {base_indirection} (should be tail RID)")
print(f"Base record schema encoding: {bin(base_schema)} (should show column 1 updated)")
print()

# Test 3: Update multiple columns
print("Test 3: Update columns 2 and 3")
result = query.update(key, None, None, 92, 95, None)
print(f"Update returned: {result}")
print(f"Next tail position: {grades_table.next_tail_position}")

# Check base schema encoding again
base_schema = grades_table.base_pages[3][base_page_idx].read(base_slot)
print(f"Base record schema encoding: {bin(base_schema)} (should show columns 1,2,3 updated)")
print()

# Test 4: Update non-existent key
print("Test 4: Update non-existent key")
result = query.update(999999, None, 100, None, None, None)
print(f"Update returned: {result} (should be False)")
print()

# Test 5: Multiple updates to same record
print("Test 5: Multiple updates to same record")
query.update(key, None, 98, None, None, None)  # Update column 1 again
query.update(key, None, None, None, None, 100)  # Update column 4

print(f"Total tail records created: {grades_table.next_tail_position}")

# Check indirection chain
tail_rid_1 = base_indirection
if tail_rid_1 in grades_table.page_directory:
    tail_page_idx, tail_slot = grades_table.page_directory[tail_rid_1]
    tail_indirection = grades_table.tail_pages[0][tail_page_idx].read(tail_slot)
    print(f"First tail record (RID={tail_rid_1}) points back to: {tail_indirection}")

print("\n=== Test Complete ===")