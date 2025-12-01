from lstore.index import Index
from lstore.page import Page
from time import time
import threading

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

        # merge
        self.merge_lock = threading.Lock()
        self.merge_thread = None
        self.updates_since_merge = 0
        self.merge_threshold = 10

        # transaction support
        self._insert_lock = threading.Lock()


    def __merge(self):
        # print("merge is happening")
        
        # Capture merge boundary atomically
        with self.merge_lock:
            merge_cutoff = self.next_tail_position
            if merge_cutoff == 0:
                return
            
            merge_tail_page_count = len(self.tail_pages[0])
        
        # Build merged base pages (no lock)
        new_base_pages = []
        for col_pages in self.base_pages:
            new_col_pages = []
            for page in col_pages:
                new_page = Page()
                new_page.num_records = page.num_records
                new_page.data = bytearray(page.data)
                new_col_pages.append(new_page)
            new_base_pages.append(new_col_pages)

        merged_tail_rids = set()
        record_updates = {}
        
        # First pass: identify the most recent tail for each base record
        most_recent_tail = {}  # base_rid -> most_recent_tail_rid
        for tail_position in range(merge_cutoff - 1, -1, -1):
            tail_page_idx = tail_position // 512
            tail_slot = tail_position % 512
            
            if tail_page_idx >= merge_tail_page_count:
                continue
            
            tail_rid = self.tail_pages[RID_COLUMN][tail_page_idx].read(tail_slot)
            if tail_rid == 0xFFFFFFFFFFFFFFFF:
                continue
            
            base_rid = self.tail_pages[BASE_RID_COLUMN][tail_page_idx].read(tail_slot)
            
            if base_rid not in self.page_directory:
                continue
            
            # Track the first tail we see for each base (most recent since we iterate backwards)
            if base_rid not in most_recent_tail:
                # Find the actual most recent by following indirection from base
                base_page_idx, base_slot = self.page_directory[base_rid]
                current_indirection = self.base_pages[INDIRECTION_COLUMN][base_page_idx].read(base_slot)
                if current_indirection != 0:
                    most_recent_tail[base_rid] = current_indirection
        
        # Second pass: merge only historical (non-most-recent) tail records
        for tail_position in range(merge_cutoff - 1, -1, -1):
            tail_page_idx = tail_position // 512
            tail_slot = tail_position % 512
            
            if tail_page_idx >= merge_tail_page_count:
                continue
            
            tail_rid = self.tail_pages[RID_COLUMN][tail_page_idx].read(tail_slot)
            if tail_rid == 0xFFFFFFFFFFFFFFFF:
                continue
            
            base_rid = self.tail_pages[BASE_RID_COLUMN][tail_page_idx].read(tail_slot)
            
            if base_rid not in self.page_directory:
                continue
            
            # Skip if this is the most recent tail for this base record
            if base_rid in most_recent_tail and tail_rid == most_recent_tail[base_rid]:
                continue
            
            # This is a historical tail record - merge it
            merged_tail_rids.add(tail_rid)
            schema_encoding = self.tail_pages[SCHEMA_ENCODING_COLUMN][tail_page_idx].read(tail_slot)
            
            if base_rid not in record_updates:
                record_updates[base_rid] = {}
            
            for col_num in range(self.num_columns):
                if col_num in record_updates[base_rid]:
                    continue
                
                if schema_encoding & (1 << col_num):
                    tail_col_idx = 5 + col_num
                    value = self.tail_pages[tail_col_idx][tail_page_idx].read(tail_slot)
                    record_updates[base_rid][col_num] = value
        
        # Apply updates to new base pages
        for base_rid, updates in record_updates.items():
            if base_rid not in self.page_directory:
                continue
            
            base_page_idx, base_slot = self.page_directory[base_rid]
            
            for col_num, value in updates.items():
                base_col_idx = 4 + col_num
                new_base_pages[base_col_idx][base_page_idx].update(base_slot, value)
            
            # Only reset if indirection points to something we merged
            current_indirection = self.base_pages[INDIRECTION_COLUMN][base_page_idx].read(base_slot)
            if current_indirection != 0 and current_indirection in merged_tail_rids:
                new_base_pages[INDIRECTION_COLUMN][base_page_idx].update(base_slot, 0)
                new_base_pages[SCHEMA_ENCODING_COLUMN][base_page_idx].update(base_slot, 0)
        
        # Atomic switchover
        with self.merge_lock:
            for base_rid in self.base_rids:
                if base_rid not in self.page_directory:
                    continue
                base_page_idx, base_slot = self.page_directory[base_rid]
                
                # Read current indirection from live base_pages
                current_indirection = self.base_pages[INDIRECTION_COLUMN][base_page_idx].read(base_slot)
                
                # If indirection points to a tail record that was not merged,
                # we need to preserve it in new_base_pages
                if current_indirection != 0 and current_indirection not in merged_tail_rids:
                    new_base_pages[INDIRECTION_COLUMN][base_page_idx].update(base_slot, current_indirection)
                    # Also preserve the schema encoding for this record
                    current_schema = self.base_pages[SCHEMA_ENCODING_COLUMN][base_page_idx].read(base_slot)
                    new_base_pages[SCHEMA_ENCODING_COLUMN][base_page_idx].update(base_slot, current_schema)
                    
            self.base_pages = new_base_pages
            
            # Build mapping of old RID -> new RID location before deleting anything
            tail_rid_mapping = {}  # old_rid -> new_location
            
            # First pass: build the mapping
            new_tail_pages = [[Page()] for _ in range(5 + self.num_columns)]
            new_next_tail_position = 0
            
            current_tail_end = self.next_tail_position
            
            for tail_position in range(current_tail_end):
                tail_page_idx = tail_position // 512
                tail_slot = tail_position % 512
                
                if tail_page_idx >= len(self.tail_pages[0]):
                    continue
                
                tail_rid = self.tail_pages[RID_COLUMN][tail_page_idx].read(tail_slot)
                
                # Keep records that were not merged and not deleted
                if tail_rid not in merged_tail_rids and tail_rid != 0xFFFFFFFFFFFFFFFF:
                    new_position = new_next_tail_position
                    new_page_idx = new_position // 512
                    new_slot = new_position % 512
                    
                    tail_rid_mapping[tail_rid] = (new_page_idx, new_slot)
                    new_next_tail_position += 1
            
            # Second pass: copy records and fix indirection pointers
            new_next_tail_position = 0
            
            for tail_position in range(current_tail_end):
                tail_page_idx = tail_position // 512
                tail_slot = tail_position % 512
                
                if tail_page_idx >= len(self.tail_pages[0]):
                    continue
                
                tail_rid = self.tail_pages[RID_COLUMN][tail_page_idx].read(tail_slot)
                
                if tail_rid not in merged_tail_rids and tail_rid != 0xFFFFFFFFFFFFFFFF:
                    new_page_idx, new_slot = tail_rid_mapping[tail_rid]
                    
                    if new_page_idx >= len(new_tail_pages[0]):
                        for col in range(5 + self.num_columns):
                            new_tail_pages[col].append(Page())
                    
                    # Copy all columns except indirection
                    for col_idx in range(5 + self.num_columns):
                        if col_idx == INDIRECTION_COLUMN:
                            # Fix indirection pointer
                            old_indirection = self.tail_pages[INDIRECTION_COLUMN][tail_page_idx].read(tail_slot)
                            
                            if old_indirection in merged_tail_rids:
                                # Points to a merged tail - reset to 0
                                new_tail_pages[INDIRECTION_COLUMN][new_page_idx].write(0)
                            elif old_indirection in tail_rid_mapping:
                                # Points to another tail that's being kept - keep the RID same
                                # (the RID itself doesn't change, just the location in page_directory)
                                new_tail_pages[INDIRECTION_COLUMN][new_page_idx].write(old_indirection)
                            else:
                                # Points to 0 or something else
                                new_tail_pages[INDIRECTION_COLUMN][new_page_idx].write(old_indirection)
                        else:
                            value = self.tail_pages[col_idx][tail_page_idx].read(tail_slot)
                            new_tail_pages[col_idx][new_page_idx].write(value)
                    
                    new_next_tail_position += 1
            
            # Update page directory with new tail positions
            for tail_rid, location in tail_rid_mapping.items():
                self.page_directory[tail_rid] = location
            
            # Remove merged tail RIDs from page directory
            for tail_rid in merged_tail_rids:
                if tail_rid in self.page_directory:
                    del self.page_directory[tail_rid]
            
            self.tail_pages = new_tail_pages
            self.next_tail_position = new_next_tail_position
            self.updates_since_merge = 0


    def trigger_merge(self):
        if self.updates_since_merge >= self.merge_threshold:
            if self.merge_thread is None or not self.merge_thread.is_alive():
                self.merge_thread = threading.Thread(target=self.__merge, daemon=True)
                self.merge_thread.start()


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
        with self.merge_lock:
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
            self.tail_pages[4][page_index].write(base_rid)  # Store base RID
            # Sixth, write user data for columns that are not None
            for i, value in enumerate(columns):
                col_index = 5 + i  # User columns start at index 5
                if value is not None:
                    self.tail_pages[col_index][page_index].write(value)
                else:
                    self.tail_pages[col_index][page_index].write(0)

            self.page_directory[rid] = (page_index, slot)

            # Increment update counter
            self.updates_since_merge += 1

        self.trigger_merge()


    def delete_record(self, rid):
        """
        Delete a record by RID. Used for transaction rollback.
        """
        if rid not in self.base_rids:
            return False

        page_index, slot = self.page_directory[rid]

        # Invalidate all tail records in chain
        base_indirection = self.base_pages[0][page_index].read(slot)
        current_tail_rid = base_indirection

        while current_tail_rid != 0:
            if current_tail_rid in self.page_directory:
                tail_page_idx, tail_slot = self.page_directory[current_tail_rid]
                next_tail_rid = self.tail_pages[0][tail_page_idx].read(tail_slot)
                self.tail_pages[1][tail_page_idx].update(tail_slot, 0xFFFFFFFFFFFFFFFF)
                current_tail_rid = next_tail_rid
            else:
                break

        # Mark base record as deleted
        self.base_pages[1][page_index].update(slot, 0xFFFFFFFFFFFFFFFF)

        # Remove from base_rids
        self.base_rids.discard(rid)

        # Remove from indexes
        for col_num in range(self.num_columns):
            if self.index.indices[col_num] is not None:
                col_idx = 4 + col_num
                value = self.base_pages[col_idx][page_index].read(slot)
                self.index._delete_entry(col_num, value, rid)

        return True