import os
from collections import OrderedDict
from pathlib import Path

class CacheManager:
    """
    Memory cache for database pages with LRU eviction strategy.
    Manages a limited number of pages in memory and handles disk persistence.
    """

    def __init__(self, max_pages=1000, storage_path=None):
        self.max_pages = max_pages
        self.storage_path = Path(storage_path) if storage_path else None

        if self.storage_path:
            self.storage_path.mkdir(parents=True, exist_ok=True)

        self.memory = OrderedDict()
        self.access_count = 0
        self.miss_count = 0
        self.evict_count = 0
        self.disk_write_count = 0

    def build_key(self, tbl, range_num, seg, pg_idx, col_idx):
        return (tbl, range_num, seg, pg_idx, col_idx)

    def build_path(self, tbl, range_num, seg, pg_idx, col_idx):
        if self.storage_path is None:
            return None

        tbl_folder = self.storage_path / tbl
        range_folder = tbl_folder / f"r{range_num}"
        seg_folder = range_folder / f"s{seg}"
        file_name = f"p{pg_idx}_c{col_idx}.dat"

        return seg_folder / file_name

    def fetch(self, tbl, range_num, seg, pg_idx, col_idx):
        key = self.build_key(tbl, range_num, seg, pg_idx, col_idx)

        if key in self.memory:
            self.memory.move_to_end(key)
            self.access_count += 1
            pg = self.memory[key]
        else:
            self.miss_count += 1

            if len(self.memory) >= self.max_pages:
                self.remove_oldest()

            pg = self.load_from_disk(tbl, range_num, seg, pg_idx, col_idx)
            self.memory[key] = pg

        pg.increment_pin()
        return pg, key

    def load_from_disk(self, tbl, range_num, seg, pg_idx, col_idx):
        from lstore.page import Page

        path = self.build_path(tbl, range_num, seg, pg_idx, col_idx)

        if path is None or not os.path.exists(path):
            return Page()

        try:
            with open(path, 'rb') as f:
                raw_data = f.read()
                return Page.deserialize(raw_data)
        except Exception as err:
            print(f"Warning: Could not load from {path}: {err}")
            return Page()

    def remove_oldest(self):
        for key, pg in list(self.memory.items()):
            if not pg.is_locked():
                if pg.modified:
                    self.save_to_disk(key, pg)

                del self.memory[key]
                self.evict_count += 1
                return

        raise RuntimeError(
            f"Cannot evict: all {len(self.memory)} pages are locked"
        )

    def free(self, key):
        if key not in self.memory:
            raise KeyError(f"Key {key} not found in cache")

        pg = self.memory[key]
        pg.decrement_pin()

        if not pg.is_locked():
            self.memory.move_to_end(key)

    def save_to_disk(self, key, pg):
        if self.storage_path is None:
            return

        path = self.build_path(*key)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "wb") as f:
            f.write(pg.serialize())

        pg.modified = False
        self.disk_write_count += 1

    def write_page(self, tbl, range_num, seg, pg_idx, col_idx):
        key = self.build_key(tbl, range_num, seg, pg_idx, col_idx)

        if key in self.memory:
            pg = self.memory[key]
            if pg.modified:
                self.save_to_disk(key, pg)

    def remove_table(self, tbl):
        keys_to_drop = [k for k in self.memory if k[0] == tbl]

        for k in keys_to_drop:
            pg = self.memory[k]
            if pg.is_locked():
                raise RuntimeError(
                    f"Cannot remove table {tbl}: page {k} is locked"
                )
            del self.memory[k]

    def write_all(self):
        for key, pg in self.memory.items():
            if pg.modified:
                self.save_to_disk(key, pg)

    def reset(self):
        self.write_all()
        self.memory.clear()

    def statistics(self):
        total = self.access_count + self.miss_count
        rate = (self.access_count / total * 100) if total > 0 else 0

        return {
            'max_pages': self.max_pages,
            'cached': len(self.memory),
            'hits': self.access_count,
            'misses': self.miss_count,
            'hit_rate': f"{rate:.2f}%",
            'evictions': self.evict_count,
            'disk_writes': self.disk_write_count,
        }

    def __repr__(self):
        stats = self.statistics()
        return (
            f"CacheManager(cached={stats['cached']}/{stats['max_pages']}, "
            f"hit_rate={stats['hit_rate']}, evictions={stats['evictions']})"
        )
