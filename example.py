import time
import numpy
import mmap
from itertools import count

from kaylee import Server

# Note, we never load the whole file into memory.
f = open('mobydick.txt')
mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

# This just enumerates all lines in the file, but is able to
# get data from disk into ZeroMQ much faster than read/writes.

def datafn():
    i = count(0)
    total = mm.size()
    while mm.tell() < total:
        yield next(i), memoryview(mm.readline())
    mm.close()

# map :: (k1,v1) -> [ (k2, v2) ]
def mapfn(k1, v):
    for w in v.bytes.split():
        yield w, 1

# reduce :: (k2, [v2]) -> [ (k3, v3) ]
def reducefn(k2, v):
    return sum(v)

# Server
s = Server()
s.connect()

s.mapfn    = mapfn
s.reducefn = reducefn
s.datafn   = datafn

start = time.time()
s.start()
stop = time.time()

print stop-start
#print s.results()
print sorted(s.results().iteritems(), key=lambda x: x[1], reverse=True)[1:25]

# Use a multiprocessing Pool example! Not the general use case
# though!
