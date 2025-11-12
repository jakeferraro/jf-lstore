from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4  # Only used in tail records


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.next_rid = 0
        self.base_rids = set()
        self.next_base_position = 0
        self.next_tail_position = 0
        self.base_pages = [[Page()] for _ in range(4 + self.num_columns)]   # 4 metadata cols, num_columns user cols
        self.tail_pages = [[Page()] for _ in range(5 + self.num_columns)]   # 5 metadata cols (includes BaseRID), num_columns user cols

    def __merge(self):
        print("merge is happening")
        pass
    
    def insert_record(self, columns):
        rid = self.next_rid
        self.base_rids.add(rid)
        self.next_rid += 1
        indirection = 0
        schema_encoding = 0     # Schema encoding on write will always be 0
        timestamp = int(time())
        position = self.next_base_position
        page_index = position // 512     
        slot = position % 512
        self.next_base_position += 1

        if page_index >= len(self.base_pages[0]):       # Create new page if needed
            for col in range(4 + self.num_columns):
                self.base_pages[col].append(Page())     # Allocate new page for all columns
        
        # Write metadata (cols 0-3)
        self.base_pages[0][page_index].write(indirection)
        self.base_pages[1][page_index].write(rid)
        self.base_pages[2][page_index].write(timestamp)
        self.base_pages[3][page_index].write(schema_encoding)
        
        # Write user data (cols 4+)
        for i, value in enumerate(columns):
            col_index = 4 + i
            self.base_pages[col_index][page_index].write(value)
        
        self.page_directory[rid] = (page_index, slot)

        # Update indexes for all indexed cols
        for col_num in range(self.num_columns):
            if self.index.indices[col_num] is not None:
                self.index._insert_entry(col_num, columns[col_num], rid)

    def _update_base_indirection(self, rid, new_indirection_val):
        page_index, slot = self.page_directory[rid]
        self.base_pages[0][page_index].update(slot, new_indirection_val)

    def _update_base_schema(self, rid, new_schema_val):
        page_index, slot = self.page_directory[rid]
        current_schema = self.base_pages[3][page_index].read(slot)
        # OR with new schema to accumulate which columns have been updated
        updated_schema = current_schema | new_schema_val
        self.base_pages[3][page_index].update(slot, updated_schema)

    def create_tail_record(self, base_rid, columns):
        # First, generate metadata vals
        rid = self.next_rid
        self.next_rid += 1
        # Use base page's current indirection val as new tail record indirection val
        base_page_index, base_slot = self.page_directory[base_rid]
        indirection = self.base_pages[0][base_page_index].read(base_slot)
        # Use bit shifting to generate schema encoding integer from columns tuple
        schema_encoding = 0
        for i, col_value in enumerate(columns):
            if col_value is not None:
                schema_encoding |= (1 << i)     # Set the i-th bit
        timestamp = int(time())
        # Second, generate page_index and slot
        position = self.next_tail_position
        page_index = position // 512
        slot = position % 512
        self.next_tail_position += 1
        # Third, check if page exists and allocate if not
        if page_index >= len(self.tail_pages[0]):
            for col in range(5 + self.num_columns):  # 5 metadata cols for tail records
                self.tail_pages[col].append(Page())
        # Fourth, update base page indirection and schema
        self._update_base_indirection(base_rid, rid)
        self._update_base_schema(base_rid, schema_encoding)
        # Fifth, write metadata columns (including BaseRID)
        self.tail_pages[0][page_index].write(indirection)
        self.tail_pages[1][page_index].write(rid)
        self.tail_pages[2][page_index].write(timestamp)
        self.tail_pages[3][page_index].write(schema_encoding)
        self.tail_pages[4][page_index].write(base_rid)  # Store BaseRID
        # Sixth, write user data for columns that are not None
        for i, value in enumerate(columns):
            col_index = 5 + i  # User columns start at index 5 now
            if value is not None:
                self.tail_pages[col_index][page_index].write(value)
            else:
                self.tail_pages[col_index][page_index].write(0)

        self.page_directory[rid] = (page_index, slot)