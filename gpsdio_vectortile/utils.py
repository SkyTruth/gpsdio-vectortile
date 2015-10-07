import msgpack
import contextlib
import struct

class Writer(object):
    def __init__(self, file):
        self.file = file
        self.packer = msgpack.Packer()

    def write(self, obj):
        self.file.write(self.packer.pack(obj))

@contextlib.contextmanager
def msgpack_open(name, mode='r'):
    if mode == 'r':
        with open(name) as f:
            yield msgpack.Unpacker(f)
    else:
        with open(name, mode) as f:
            yield Writer(f)

def float2bits(f):
    return struct.unpack('>l', struct.pack('>f', f))[0]

def bits2float(b):
    return struct.unpack('>f', struct.pack('>l', b))[0]
