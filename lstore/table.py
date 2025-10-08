from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


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
        self.base_pages = [[Page()] for _ in range(4 + self.num_columns)]   # 4 metadata cols, num_columns user cols
        self.tail_pages = [[Page()] for _ in range(4 + self.num_columns)]

    def __merge(self):
        print("merge is happening")
        pass
    
    def insert_record(self, columns):
        rid = self.next_rid
        self.next_rid += 1
        indirection = 0
        schema_encoding = 0     # Schema encoding on write will always be 0
        timestamp = int(time())
        page_index = rid // 512     
        slot = rid % 512

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