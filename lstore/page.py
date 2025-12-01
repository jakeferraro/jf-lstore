class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.modified = False
        self.lock_count = 0

    def has_capacity(self):
        return self.num_records < 512

    def write(self, value):
        position = self.num_records * 8
        if value < 0:
            byte_val = value.to_bytes(8, byteorder='big', signed=True)
        else:
            byte_val = value.to_bytes(8, byteorder='big', signed=False)
        self.data[position:position+8] = byte_val
        self.num_records += 1
        self.modified = True

    def read(self, index):
        position = index * 8
        data_bytes = self.data[position:position+8]
        return int.from_bytes(data_bytes, byteorder='big', signed=True)

    def update(self, index, value):
        position = index * 8
        if value < 0:
            byte_val = value.to_bytes(8, byteorder='big', signed=True)
        else:
            byte_val = value.to_bytes(8, byteorder='big', signed=False)
        self.data[position:position+8] = byte_val
        self.modified = True

    def increment_pin(self):
        self.lock_count += 1

    def decrement_pin(self):
        if self.lock_count == 0:
            raise RuntimeError("Cannot decrement pin on unlocked page")
        self.lock_count -= 1

    def is_locked(self):
        return self.lock_count > 0

    def serialize(self):
        header = self.num_records.to_bytes(8, byteorder='big', signed=False)
        return header + bytes(self.data)

    @staticmethod
    def deserialize(raw_data):
        pg = Page()
        pg.num_records = int.from_bytes(raw_data[0:8], byteorder='big', signed=False)
        pg.data = bytearray(raw_data[8:8+4096])
        pg.modified = False
        return pg