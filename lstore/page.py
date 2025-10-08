
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < 512

    def write(self, value):
        position = self.num_records * 8
        byte_val = value.to_bytes(8, byteorder='big')
        self.data[position:position+8] = byte_val
        self.num_records += 1

    def read(self, index):
        position = index * 8
        data_bytes = self.data[position:position+8]
        return int.from_bytes(data_bytes, byteorder='big') 