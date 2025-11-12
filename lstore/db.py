from lstore.table import Table
import os
import struct

class Database():

    def __init__(self):
        self.tables = []
        self.path = None


    # Not required for milestone1
    def open(self, path):
        """
        Open database from disk. Load all tables from the given path.
        If path doesn't exist, create it (for new database).
        """
        self.path = path

        # Create dir if it doesn't exist
        if not os.path.exists(path):
            os.makedirs(path)
            return
        
        # Load all tables from disk
        for filename in os.listdir(path):
            if filename.endswith('_meta.bin'):  # Each table has a metadata file
                table_name = filename[:-9]      # Remove '_meta.bin'
                table = self._load_table(table_name)
                if table:
                    self.tables.append(table)


    def close(self):
        """
        Close database and persist all tables to disk
        """
        if self.path is None:
            return
        
        for table in self.tables:
            self._save_table(table)


    def _save_table(self, table):
        """Save a single table to disk."""
        base_path = os.path.join(self.path, table.name)
        
        # Save metadata
        meta_path = base_path + '_meta.bin'
        with open(meta_path, 'wb') as f:
            # Write table metadata
            name_bytes = table.name.encode('utf-8')
            f.write(struct.pack('I', len(name_bytes)))
            f.write(name_bytes)
            f.write(struct.pack('III', table.num_columns, table.key, table.next_rid))
            f.write(struct.pack('II', table.next_base_position, table.next_tail_position))
            
            # Write base_rids set
            f.write(struct.pack('I', len(table.base_rids)))
            for rid in table.base_rids:
                f.write(struct.pack('Q', rid))
            
            # Write indexed columns list
            indexed_cols = [i for i in range(table.num_columns) if table.index.indices[i] is not None]
            f.write(struct.pack('I', len(indexed_cols)))
            for col in indexed_cols:
                f.write(struct.pack('I', col))
        
        # Save base pages
        base_pages_path = base_path + '_base.bin'
        with open(base_pages_path, 'wb') as f:
            # Write number of columns
            num_base_cols = len(table.base_pages)
            f.write(struct.pack('I', num_base_cols))
            
            # For each column, write number of pages and page data
            for col_pages in table.base_pages:
                f.write(struct.pack('I', len(col_pages)))
                for page in col_pages:
                    f.write(struct.pack('I', page.num_records))
                    f.write(page.data)
        
        # Save tail pages
        tail_pages_path = base_path + '_tail.bin'
        with open(tail_pages_path, 'wb') as f:

            num_tail_cols = len(table.tail_pages)
            f.write(struct.pack('I', num_tail_cols))
            
            for col_pages in table.tail_pages:
                f.write(struct.pack('I', len(col_pages)))
                for page in col_pages:
                    f.write(struct.pack('I', page.num_records))
                    f.write(page.data)
        
        # Save page directory
        dir_path = base_path + '_dir.bin'
        with open(dir_path, 'wb') as f:
            f.write(struct.pack('I', len(table.page_directory)))
            for rid, (page_idx, slot) in table.page_directory.items():
                f.write(struct.pack('QII', rid, page_idx, slot))
    

    def _load_table(self, table_name):
        """Load a single table from disk."""
        from lstore.page import Page
        
        base_path = os.path.join(self.path, table_name)
        meta_path = base_path + '_meta.bin'
        
        if not os.path.exists(meta_path):
            return None
        
        # Load metadata
        with open(meta_path, 'rb') as f:
            # Read table name
            name_len = struct.unpack('I', f.read(4))[0]
            name = f.read(name_len).decode('utf-8')
            
            # Read table metadata
            num_columns, key, next_rid = struct.unpack('III', f.read(12))
            next_base_pos, next_tail_pos = struct.unpack('II', f.read(8))
            
            # Read base_rids
            num_base_rids = struct.unpack('I', f.read(4))[0]
            base_rids = set()
            for _ in range(num_base_rids):
                rid = struct.unpack('Q', f.read(8))[0]
                base_rids.add(rid)
            
            # Read indexed columns
            num_indexed = struct.unpack('I', f.read(4))[0]
            indexed_cols = []
            for _ in range(num_indexed):
                col = struct.unpack('I', f.read(4))[0]
                indexed_cols.append(col)
        
        # Create table object
        table = Table(name, num_columns, key)
        table.next_rid = next_rid
        table.next_base_position = next_base_pos
        table.next_tail_position = next_tail_pos
        table.base_rids = base_rids
        
        # Load base pages
        base_pages_path = base_path + '_base.bin'
        with open(base_pages_path, 'rb') as f:
            num_base_cols = struct.unpack('I', f.read(4))[0]
            table.base_pages = []
            
            for _ in range(num_base_cols):
                num_pages = struct.unpack('I', f.read(4))[0]
                col_pages = []
                for _ in range(num_pages):
                    page = Page()
                    page.num_records = struct.unpack('I', f.read(4))[0]
                    page.data = bytearray(f.read(4096))
                    col_pages.append(page)
                table.base_pages.append(col_pages)
        
        # Load tail pages
        tail_pages_path = base_path + '_tail.bin'
        with open(tail_pages_path, 'rb') as f:
            num_tail_cols = struct.unpack('I', f.read(4))[0]
            table.tail_pages = []
            
            for _ in range(num_tail_cols):
                num_pages = struct.unpack('I', f.read(4))[0]
                col_pages = []
                for _ in range(num_pages):
                    page = Page()
                    page.num_records = struct.unpack('I', f.read(4))[0]
                    page.data = bytearray(f.read(4096))
                    col_pages.append(page)
                table.tail_pages.append(col_pages)
        
        # Load page directory
        dir_path = base_path + '_dir.bin'
        with open(dir_path, 'rb') as f:
            num_entries = struct.unpack('I', f.read(4))[0]
            table.page_directory = {}
            for _ in range(num_entries):
                rid, page_idx, slot = struct.unpack('QII', f.read(16))
                table.page_directory[rid] = (page_idx, slot)
        
        # Rebuild indexes for indexed columns
        for col in indexed_cols:
            table.index.create_index(col)
        
        return table


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        self.tables = [t for t in self.tables if t.name != name]
        return True

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        return None
