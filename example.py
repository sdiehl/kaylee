import time
from kaylee import Server

# Example
# -----------------------------------------------

f = open('mobydick.txt')

data = dict(enumerate(f.readlines()))

def mapfn(k, v):
    for w in v.split():
        yield w, 1

def reducefn(k, v):
    return sum(v)

# Server
s = Server()
s.connect()

s.mapfn    = mapfn
s.reducefn = reducefn
s.data     = data

start = time.time()
s.start()
stop = time.time()

print stop-start
#print s.results()
