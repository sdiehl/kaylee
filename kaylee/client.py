import sys
import uuid
import numpy
import marshal
import types
import logging
from itertools import imap
from utils import zmq_addr

import gevent
import zmq.green as zmq

try:
    import msgpack as srl
except ImportError:
    import cPickle as srl

# Server instructions
# -------------------
MAP      = 'map'
REDUCE   = 'reduce'
DONE     = 'done'
BYTECODE = 'bytecode'

# Client instructions
# -------------------
CONNECT     = 'connect'
MAPATOM     = 'mapdone'
MAPCHUNK    = 'mapkeydone'
REDUCEATOM  = 'reducedone'

class Client(object):
    """
    The MapReduce worker is a stateless worker, it receives a
    value over ZMQ, calls the map/reduce function and yields it
    back to the socket as quickly as possible.
    """

    def __init__(self):
        self.worker_id = str(uuid.uuid4())

        self.push_socket = None
        self.pull_socket = None
        self.ctrl_socket = None

        self.have_bytecode = False

        self.mapfn = None
        self.reducefn = None
        self.datasource = None

        logging.basicConfig(logging=logging.DEBUG)
        logging.getLogger("").setLevel(logging.INFO)
        self.logging = logging

    def connect(self, push_addr = None,
                      pull_addr = None,
                      ctrl_addr = None):

        c = zmq.Context()

        if not pull_addr:
            addr = zmq_addr(5555, transport='tcp')

        self.pull_socket = c.socket(zmq.PULL)
        self.pull_socket.connect(addr)

        if not push_addr:
            addr = zmq_addr(6666, transport='tcp')

        self.push_socket = c.socket(zmq.PUSH)
        self.push_socket.connect(addr)

        if not ctrl_addr:
            addr = zmq_addr(7777, transport='tcp')

        self.ctrl_socket = c.socket(zmq.ROUTER)
        self.ctrl_socket.setsockopt(zmq.IDENTITY, self.worker_id)
        self.ctrl_socket.connect(addr)

    def start(self):
        self.logging.info('Started Worker %s' % self.worker_id)
        self.collect()

    def kill(self):
        self.ctrl_socket.close()
        self.pull_socket.close()
        self.push_socket.close()
        self.logging.info('Stopped Worker')

    def collect(self):
        poller = zmq.Poller()
        poller.register(self.pull_socket, zmq.POLLIN)
        poller.register(self.ctrl_socket, zmq.POLLIN)

        pull_socket = self.pull_socket
        ctrl_socket = self.ctrl_socket

        while True:

            if self.have_bytecode:

                try:
                    events = dict(poller.poll())
                except zmq.ZMQError:
                    # Die gracefully if the user sends a SIGQUIT
                    self._kill()
                    break

                if events.get(pull_socket) == zmq.POLLIN:

                    command = self.pull_socket.recv(flags=zmq.SNDMORE)
                    key = self.pull_socket.recv(flags=zmq.SNDMORE)
                    data = self.pull_socket.recv(copy=False)

                    if command == MAP:
                        self.call_mapfn(key, data)
                    elif command == REDUCE:
                        self.call_reducefn(key, data)

                if events.get(ctrl_socket) == zmq.POLLIN:
                    worker_id, command = self.ctrl_socket.recv_multipart()
                    if command == DONE:
                        self.kill()
                        break

            else:
                self.logging.info('Waiting for server')

                # Associate with the server
                self.push_socket.send_multipart([CONNECT, self.worker_id])

                # Wait for the server to route us the bytecode,
                # then start the work cycle
                worker_id, payload = self.ctrl_socket.recv_multipart()
                command, (mapbc, reducebc) = srl.loads(payload)

                assert command == BYTECODE
                self.set_bytecode(mapbc, reducebc)
                self.logging.info('Received Bytecode')

    def set_bytecode(self, mapbc, reducebc):
        '''
        Load the bytecode sent by the server and flag that we are
        ready for work.
        '''

        self.mapfn = types.FunctionType(
            marshal.loads(mapbc),
            globals(),
            'mapfn'
        )
        self.reducefn = types.FunctionType(
            marshal.loads(reducebc),
            globals(),
            'reducefn'
        )

        self.have_bytecode = True

    def set_llvm(self, mapbc, reducebc, mapsig=None, reducesig=None):
        from numba.translate import Translate

        ret_type, arg_types = mapsig

        mapt = Translate(mapbc, ret_type, arg_types)
        mapt.translate()

        ret_type, arg_types = reducesig

        reducet = Translate(mapbc, ret_type, arg_types)
        reducet.translate()

        self.mapfn = mapt.get_ctypes_func(llvm=True)
        self.reducefn = reducet.get_ctypes_func(llvm=True)

        self.have_bytecode = True

    def call_mapfn(self, key, value):
        # TODO: specify the granulariy of chunks to flush to the
        # server.

        for k1, v1 in self.mapfn(key, value):
            print 'mapping', k1, v1
            self.push_socket.send_multipart([MAPATOM, key, k1], flags=zmq.SNDMORE)
            self.push_socket.send(srl.dumps(v1))

        # Signal that k1 has been completed. The system sees this
        # as an atomic unit of work.
        self.push_socket.send(MAPCHUNK, flags=zmq.SNDMORE)
        self.push_socket.send(key)

    def call_reducefn(self, key, value):
        # Lazily deserialize since not all the output may be
        # needed by the reducer so don't waste cycles.
        it = imap(srl.loads, srl.loads(value))
        results = self.reducefn(key, it)

        print 'reducing', key, results
        self.push_socket.send(REDUCEATOM, flags=zmq.SNDMORE)
        self.push_socket.send(key, flags=zmq.SNDMORE)

        if isinstance(results, numpy.ndarray):
            self.push_socket.send(results, copy=False)
        else:
            self.push_socket.send(srl.dumps(results))

if __name__ == "__main__":
    c = Client()
    c.connect()
    c.start()
