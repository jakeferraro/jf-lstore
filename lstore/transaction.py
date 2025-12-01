from lstore.table import Table, Record
from lstore.index import Index
import threading
import time
import random

class LockType:
    SHARED = 0
    EXCLUSIVE = 1

class RecordLockManager:
    def __init__(self):
        self.guard = threading.Lock()
        self.record_locks = {}

    def try_lock(self, txn, record_id, lock_type):
        with self.guard:
            if record_id not in self.record_locks:
                self.record_locks[record_id] = {"type": lock_type, "holders": {txn}}
                return True

            lock_info = self.record_locks[record_id]
            current_type = lock_info["type"]
            holders = lock_info["holders"]

            if current_type == LockType.SHARED:
                if lock_type == LockType.SHARED:
                    holders.add(txn)
                    return True
                elif lock_type == LockType.EXCLUSIVE and holders == {txn}:
                    lock_info["type"] = LockType.EXCLUSIVE
                    return True
            elif current_type == LockType.EXCLUSIVE and holders == {txn}:
                return True

            return False

    def release_all_locks(self, txn):
        with self.guard:
            to_remove = []
            for record_id, lock_info in self.record_locks.items():
                holders = lock_info["holders"]
                holders.discard(txn)
                if not holders:
                    to_remove.append(record_id)

            for record_id in to_remove:
                del self.record_locks[record_id]

GLOBAL_LOCK_MGR = RecordLockManager()

class AbortSignal(Exception):
    pass

class Transaction:

    def __init__(self):
        self.queries = []
        self.rollback_log = []
        self.held_locks = {}

    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

    def run(self, retry_limit=10):
        attempt = 0

        while True:
            self.rollback_log.clear()
            self.held_locks.clear()

            try:
                for query_func, tbl, args in self.queries:
                    operation = query_func.__name__

                    if operation == "insert":
                        self.handle_insert(query_func, tbl, args)
                    elif operation == "update":
                        self.handle_update(query_func, tbl, args)
                    elif operation == "delete":
                        self.handle_delete(query_func, tbl, args)
                    else:
                        self.handle_read(query_func, tbl, args)

                return self.commit()

            except AbortSignal as e:
                print(f"Transaction {id(self)} aborted on attempt {attempt + 1}: {e}")
                self.abort()
                attempt += 1

                if attempt >= retry_limit:
                    return False

                time.sleep(random.uniform(0.001, 0.01 * attempt))
                continue

            except Exception as e:
                print(f"Transaction {id(self)} error on attempt {attempt + 1}: {type(e).__name__}: {e}")
                self.abort()
                attempt += 1

                if attempt >= retry_limit:
                    return False

                continue

    def abort(self):
        for entry in reversed(self.rollback_log):
            operation_type = entry["operation"]
            tbl = entry["table"]

            try:
                if operation_type == "insert":
                    record_id = entry["record_id"]
                    tbl.delete_record(record_id)

                elif operation_type == "update":
                    record_id = entry["record_id"]
                    old_ind = entry["old_indirection"]
                    old_sch = entry["old_schema"]
                    self.restore_metadata(tbl, record_id, old_ind, old_sch)

                elif operation_type == "delete":
                    record_id = entry["record_id"]
                    old_vals = entry["old_values"]
                    old_ind = entry["old_indirection"]
                    old_sch = entry["old_schema"]

                    tbl.index.add(record_id, old_vals)
                    self.restore_metadata(tbl, record_id, old_ind, old_sch)

            except Exception as err:
                print(f"Rollback failed for {operation_type} on record {entry.get('record_id')}: {err}")

        self.release_locks()
        return False

    def commit(self):
        self.rollback_log.clear()
        self.release_locks()
        return True

    def acquire_lock(self, record_id, lock_type=LockType.EXCLUSIVE):
        if record_id in self.held_locks:
            held_type = self.held_locks[record_id]

            if held_type == LockType.EXCLUSIVE or held_type == lock_type:
                return True

            success = GLOBAL_LOCK_MGR.try_lock(id(self), record_id, lock_type)
            if success:
                self.held_locks[record_id] = lock_type
            return success

        success = GLOBAL_LOCK_MGR.try_lock(id(self), record_id, lock_type)
        if success:
            self.held_locks[record_id] = lock_type
        return success

    def release_locks(self):
        if self.held_locks:
            GLOBAL_LOCK_MGR.release_all_locks(id(self))
            self.held_locks.clear()

    def handle_read(self, query_func, tbl, args):
        operation = query_func.__name__
        record_ids = []

        if operation in ("select", "select_version"):
            key, key_col = args[0], args[1]
            record_ids = tbl.index.locate(key_col, key)
        elif operation in ("sum", "sum_version"):
            start, end = args[0], args[1]
            record_ids = tbl.index.locate_range(start, end, tbl.key)

        for rid in record_ids:
            if not self.acquire_lock(rid, LockType.SHARED):
                raise AbortSignal(f"{operation}: cannot acquire shared lock on {rid}")

        result = query_func(*args)
        if result is False:
            raise AbortSignal(f"{operation} returned False")

        return result

    def handle_insert(self, query_func, tbl, args):
        with tbl._insert_lock:
            new_rid = tbl.next_rid

            if not self.acquire_lock(new_rid, LockType.EXCLUSIVE):
                raise AbortSignal("Cannot acquire lock for insert")

            result = query_func(*args)
            if result is False:
                raise AbortSignal("Insert failed")

            self.rollback_log.append({"operation": "insert", "table": tbl, "record_id": new_rid})

    def handle_update(self, query_func, tbl, args):
        key = args[0]
        record_ids = tbl.index.locate(tbl.key, key)

        if not record_ids:
            raise AbortSignal(f"Update: no record with key {key}")

        rid = record_ids[0]

        if not self.acquire_lock(rid, LockType.EXCLUSIVE):
            raise AbortSignal(f"Update: cannot acquire lock on {rid}")

        try:
            page_idx, slot = tbl.page_directory[rid]
            old_indirection = tbl.base_pages[0][page_idx].read(slot)
            old_schema = tbl.base_pages[3][page_idx].read(slot)
            old_values = []

        except Exception:
            raise AbortSignal(f"Update: cannot read record {rid}")

        result = query_func(*args)
        if result is False:
            raise AbortSignal("Update failed")

        self.rollback_log.append({
            "operation": "update",
            "table": tbl,
            "record_id": rid,
            "old_values": old_values,
            "old_indirection": old_indirection,
            "old_schema": old_schema,
        })

    def handle_delete(self, query_func, tbl, args):
        key = args[0]
        record_ids = tbl.index.locate(tbl.key, key)

        if not record_ids:
            raise AbortSignal(f"Delete: no record with key {key}")

        rid = record_ids[0]

        if not self.acquire_lock(rid, LockType.EXCLUSIVE):
            raise AbortSignal(f"Delete: cannot acquire lock on {rid}")

        try:
            page_idx, slot = tbl.page_directory[rid]
            old_indirection = tbl.base_pages[0][page_idx].read(slot)
            old_schema = tbl.base_pages[3][page_idx].read(slot)
            old_values = []

        except Exception:
            raise AbortSignal(f"Delete: cannot read record {rid}")

        result = query_func(*args)
        if result is False:
            raise AbortSignal("Delete failed")

        self.rollback_log.append({
            "operation": "delete",
            "table": tbl,
            "record_id": rid,
            "old_values": old_values,
            "old_indirection": old_indirection,
            "old_schema": old_schema,
        })

    def restore_metadata(self, tbl, record_id, indirection, schema):
        page_idx, slot = tbl.page_directory[record_id]
        tbl.base_pages[0][page_idx].update(slot, indirection)
        tbl.base_pages[3][page_idx].update(slot, schema)
        return True
