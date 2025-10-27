from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        key_col_index = 4 + self.table.key

        # Find record with primary key
        for rid in list(self.table.base_rids):
            page_index, slot = self.table.page_directory[rid]
            value = self.table.base_pages[key_col_index][page_index].read(slot)
            
            if value == primary_key:
                # Mark base record as deleted by setting RID to max val
                self.table.base_pages[1][page_index].update(slot, 0xFFFFFFFFFFFFFFFF)
                # Remove from base_rids set
                self.table.base_rids.discard(rid)

                return True
            
        return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        self.table.insert_record(columns)

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        records = []
        col_index = 4 + search_key_index

        for rid in self.table.base_rids:
            # Read search column
            page_index, slot = self.table.page_directory[rid]
            value = self.table.base_pages[col_index][page_index].read(slot)

            if value == search_key:     # Matching base record found
                # Initalize result columns
                result_columns = [None] * self.table.num_columns
                # Track which columns are needed
                columns_needed = set()
                for i, is_projected in enumerate(projected_columns_index):
                    if is_projected == 1:
                        columns_needed.add(i)
                # Start with base record's indirection
                base_page_index, base_slot = self.table.page_directory[rid]
                current_tail_rid = self.table.base_pages[0][base_page_index].read(base_slot)
                # Traverse tail records
                while current_tail_rid != 0 and len(columns_needed) > 0:
                    tail_page_index, tail_slot = self.table.page_directory[current_tail_rid]
                    # Read schema encoding to see which columns are in this tail record
                    schema_encoding = self.table.tail_pages[3][tail_page_index].read(tail_slot)
                    # Check each column we still need
                    for col_num in list(columns_needed):
                        # Check if this col was updated in this tail record
                        if schema_encoding & (1 << col_num):
                            # Read value from tail
                            tail_col_index = 4 + col_num
                            result_columns[col_num] = self.table.tail_pages[tail_col_index][tail_page_index].read(tail_slot)
                            columns_needed.remove(col_num)
                    current_tail_rid = self.table.tail_pages[0][tail_page_index].read(tail_slot)
                # Fill in remaining columns from base record
                for col_num in columns_needed:
                    base_col_index = 4 + col_num
                    result_columns[col_num] = self.table.base_pages[base_col_index][base_page_index].read(base_slot)

                key_val = result_columns[self.table.key]
                record = Record(rid, key_val, result_columns)
                records.append(record)

        return records
                
                

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        key_col_index = 4 + self.table.key      # Metadata offset
        for rid in self.table.base_rids:
            page_index, slot = self.table.page_directory[rid]
            value = self.table.base_pages[key_col_index][page_index].read(slot)

            if value == primary_key:
                self.table.create_tail_record(rid, columns)
                return True
            
        return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total = 0
        key_col_index = 4 + self.table.key
        agg_col_index = 4 + aggregate_column_index
        found_any = False

        # Iterate through base records
        for rid in self.table.base_rids:
            page_index, slot = self.table.page_directory[rid]
            key_val = self.table.base_pages[key_col_index][page_index].read(slot)

            if start_range <= key_val <= end_range:
                found_any = True
                # Get latest val for agg column
                base_page_index, base_slot = self.table.page_directory[rid]
                current_tail_rid = self.table.base_pages[0][base_page_index].read(base_slot)

                value_found = False
                # Traverse tail records to find latest val for this col
                while current_tail_rid != 0 and not value_found:
                    tail_page_index, tail_slot = self.table.page_directory[current_tail_rid]
                    schema_encoding = self.table.tail_pages[3][tail_page_index].read(tail_slot)
                    # Check if this col was updated in this tail record
                    if schema_encoding & (1 << aggregate_column_index):
                        value = self.table.tail_pages[agg_col_index][tail_page_index].read(tail_slot)
                        total += value
                        value_found = True
                    # Move to previous tail record
                    current_tail_rid = self.table.tail_pages[0][tail_page_index].read(tail_slot)
                # If not found in tail records, get from base
                if not value_found:
                    value = self.table.base_pages[agg_col_index][page_index].read(slot)
                    total += value

        if not found_any:
            return False
        
        return total


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
