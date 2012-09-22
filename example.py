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

# Do an HDF5 data source example!

# map :: a -> [ (k1, v1) ]
def datafn():
    i = count(0)
    total = mm.size()
    while mm.tell() < total:
        yield next(i), memoryview(mm.readline())
    mm.close()

# MapReduce can be thought of on a high level as being a list
# homomorphism that can be written as a composition of two functions (
# Reduce . Map ) . It is parallelizable because of the associativity of
# the of map and reduce operations.
#
#   MapReduce :: [(k1, v1)] -> [(k3, v3)]
#   MapReduce = Reduce .  Map

#   MapReduce :: a -> [(k3, v3)]
#   MapReduce = reducefn . shuffle . mapfn . datafn

# The implementation provides two functions
# split ( datafn ) and shuffle.

# map :: (k1,v1) -> [ (k2, v2) ]
def mapfn(k1, v):
    for w in v.bytes.split():
        yield w, 1

# shuffle :: [ (k2, v2) ] -> [(k2, [v2])]

# In the Haskell notation
# pmap reducefn ( shuffle ( pmap mapfn ( split a ) ) )

# reduce :: (k2, [v2]) -> [ (k3, v3) ]
def reducefn(k2, v):
    return sum(v)

# Server
s = Server()
s.connect()

# yaml config
# Datastore backend, Redis kaylee://

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
