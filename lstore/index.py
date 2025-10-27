"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""
from sortedcontainers import SortedDict

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table

        # Create index on primary key col by default
        self.indices[table.key] = SortedDict()


    def locate(self, column, value):
        """
        Returns a list of RIDs for all records with the given value in the specified column.
        Returns empty list if no records found or if column is not indexed.
        """
        if self.indices[column] is None:
            return []
        
        # SortedDict maps value -> list of RIDs
        return self.indices[column].get(value, [])


    def locate_range(self, begin, end, column):
        """
        Returns a list of RIDs for all records with values between begin and end (inclusive)
        in the specified column. Returns empty list if column is not indexed.
        """
        if self.indices[column] is None:
            return []
        
        rids = []
        # Use SortedDict's irange for range queries
        for value in self.indices[column].irange(begin, end):
            rids.extend(self.indices[column][value])
        
        return rids


    def create_index(self, column_number):
        """
        Creates an index on the specified column.
        Scans all existing records and builds the index.
        """
        if column_number < 0 or column_number >= self.table.num_columns:
            return False
        
        # Initialize new SortedDict for this column
        self.indices[column_number] = SortedDict()
        
        # Build index from existing base records
        col_index = 4 + column_number  # Offset by metadata columns
        
        for rid in self.table.base_rids:
            page_index, slot = self.table.page_directory[rid]
            value = self.table.base_pages[col_index][page_index].read(slot)
            
            # Add RID to the list for this val
            if value not in self.indices[column_number]:
                self.indices[column_number][value] = []
            self.indices[column_number][value].append(rid)
        
        return True


    def drop_index(self, column_number):
        """
        Drops the index on the specified column.
        """
        if column_number < 0 or column_number >= self.table.num_columns:
            return False
        
        self.indices[column_number] = None
        return True
    

    def _insert_entry(self, column_number, value, rid):
        """
        Adds an entry to the index for the given column.
        Called internally when inserting records.
        """
        if self.indices[column_number] is None:
            return
        
        if value not in self.indices[column_number]:
            self.indices[column_number][value] = []
        self.indices[column_number][value].append(rid)
    

    def _delete_entry(self, column_number, value, rid):
        """
        Removes an entry from the index for the given column.
        Called internally when deleting records.
        """
        if self.indices[column_number] is None:
            return
        
        if value in self.indices[column_number]:
            if rid in self.indices[column_number][value]:
                self.indices[column_number][value].remove(rid)
            # Remove empty lists
            if len(self.indices[column_number][value]) == 0:
                del self.indices[column_number][value]
