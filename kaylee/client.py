import sys
import uuid
import numpy
import marshal
import types
import logging

import gevent
import zmq.green as zmq

try:
    import msgpack as srl
except ImportError:
    import cPickle as srl

from collections import defaultdict

class Client(object):

    def __init__(self):

        self.worker_id = str(uuid.uuid4())
        self.push_socket = None
        self.pull_socket = None
        self.ctrl_socket = None

        self.threaded = False
        self.have_bytecode = False

        self.mapfn = None
        self.reducefn = None
        self.datasource = None

        logging.basicConfig(logging=logging.DEBUG,
                            format="%(asctime)s [%(levelname)s] %(message)s")
        logging.getLogger("").setLevel(logging.INFO)
        self.logging = logging

    def connect(self, push_addr    = None,
                      pull_addr    = None,
                      control_addr = None):

            c = zmq.Context()

            # Pull tasks across manager
            if not pull_addr:
                prot = 'tcp://'
                ip   = '127.0.0.1'
                port = '5555'
                addr = ''.join([prot,ip,':',port])
            elif len(pull_addr) > 1:
                prot, ip, port = pull_addr
                addr = ''.join([prot,ip,':',port])
            else:
                addr = pull_addr

            print addr

            self.pull_socket = c.socket(zmq.PULL)
            self.pull_socket.connect(addr)

            # Pull tasks across manager
            if not push_addr:
                prot = 'tcp://'
                ip   = '127.0.0.1'
                port = '6666'
                addr = ''.join([prot,ip,':',port])
            elif len(push_addr) > 1:
                prot, ip, port = push_addr
                addr = ''.join([prot,ip,':',port])
            else:
                addr = push_addr

            print addr

            self.push_socket = c.socket(zmq.PUSH)
            self.push_socket.connect(addr)

            # Pull tasks across manager
            if not control_addr:
                prot = 'tcp://'
                ip   = '127.0.0.1'
                port = '7777'
                addr = ''.join([prot,ip,':',port])
            elif len(control_addr) > 1:
                prot, ip, port = control_addr
                addr = ''.join([prot,ip,':',port])
            else:
                addr = control_addr

            print addr

            self.ctrl_socket = c.socket(zmq.ROUTER)
            self.ctrl_socket.setsockopt(zmq.IDENTITY, self.worker_id)
            self.ctrl_socket.connect(addr)

    def start(self):
        ''' Start processing work '''
        self.logging.info('Started Worker %s' % self.worker_id)
        self.collect()

    def _kill(self):
        self.ctrl_socket.close()
        self.pull_socket.close()
        self.push_socket.close()

        self.ctrl_socket = None
        self.pull_socket = None
        self.push_socket = None
        self.logging.info('Stopped Worker')

        sys.exit(0)

    def collect(self):
        poller = zmq.Poller()
        poller.register(self.pull_socket, zmq.POLLIN)
        poller.register(self.ctrl_socket, zmq.POLLIN)

        # multiplex the pull and control ports
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
                    payload = (key, data)

                    self.process_command(command, payload)

                if events.get(ctrl_socket) == zmq.POLLIN:
                    worker_id, command = self.ctrl_socket.recv_multipart()
                    self.process_command(command, data)

            else:
                self.logging.info('Waiting for server')

                msg = srl.dumps(('connect', self.worker_id))
                self.push_socket.send_multipart(['connect', self.worker_id])

                worker_id, payload = self.ctrl_socket.recv_multipart()
                command, (mapbc, reducebc) = srl.loads(payload)

                assert command == 'bytecode'
                self.set_bytecode(mapbc, reducebc)
                self.logging.info('Received Bytecode')

    def send_command(self, command, data=None):
        '''
        Push a command to the server.
        '''
        if data:
            msg = srl.dumps((command, data))
            self.push_socket.send(msg)
        else:
            msg = command
            self.push_socket.send(msg)

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

    def on_done(self, command=None, data=None):
        self._kill()

    def call_mapfn(self, command, data):
        #results = defaultdict(list)
        key, value = data

        for k, v in self.mapfn(key, value):
            print 'mapping', k, v
            # Probably don't actually want to do this, but
            # instead collect up a temporray batch and then do a
            # tight loop where we send everything.

            self.push_socket.send('mapdone', flags=zmq.SNDMORE)
            self.push_socket.send(key, flags=zmq.SNDMORE)
            self.push_socket.send(k, flags=zmq.SNDMORE)
            self.push_socket.send(srl.dumps(v))
            #results[k].append(v)

        self.push_socket.send('keydone', flags=zmq.SNDMORE)
        self.push_socket.send(key)

        #print 'mapping', key
        #import pdb; pdb.set_trace()

        #if isinstance(results, numpy.ndarray):
            #self.push_socket.send(results, copy=False)
        #else:
            #self.push_socket.send(srl.dumps(results))

    def call_reducefn(self, command, data):
        key, value = data

        from itertools import imap
        it = imap(srl.loads, srl.loads(value))

        results = self.reducefn(key, it)

        print 'reducing', key
        self.push_socket.send('reducedone', flags=zmq.SNDMORE)
        self.push_socket.send(key, flags=zmq.SNDMORE)

        if isinstance(results, numpy.ndarray):
            self.push_socket.send(results, copy=False)
        else:
            self.push_socket.send(srl.dumps(results))

    def process_command(self, command, payload=None):
        self.commands[command](self, command, payload)

    commands = {
        'done'     : on_done,
        'map'      : call_mapfn,
        'reduce'   : call_reducefn,
    }

if __name__ == "__main__":
    c = Client()
    c.connect()
    c.start()
