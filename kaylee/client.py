import sys
import uuid
import cPickle as pickle
import marshal
import types
import logging
import gevent
from gevent_zeromq import zmq
from utils import cat
from collections import defaultdict

class Client:

    def __init__(self):

        self.worker_id = str(uuid.uuid4())
        self.push_socket = None
        self.pull_socket = None
        self.control_socket = None
        self.delim = '::'

        # only listen for instructions for this specific worker
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

            self.control_socket = c.socket(zmq.SUB)
            self.control_socket.connect(addr)
            self.control_socket.setsockopt(zmq.SUBSCRIBE, self.worker_id)

    def spawn(self):
        self.threaded = True

    def start(self):
        ''' Start processing work '''
        self.logging.info('Started Worker %s' % self.worker_id)
        self.collect()

    def _kill(self):
        self.logging.info('Stopped Worker')

        if self.threaded:
            gevent.getcurrent().kill()
        else:
            sys.exit(1)

    def collect(self):
        self.register()
        poller = zmq.Poller()
        poller.register(self.pull_socket, zmq.POLLIN)
        poller.register(self.control_socket, zmq.POLLIN)

        # multiplex the pull and control ports
        pull_socket = self.pull_socket
        control_socket = self.control_socket

        while True:
            # Wait until the server pushes bytecode to use to
            # listening for data, ( this is a race condition
            # otherwise )
            if self.have_bytecode:
                try:
                    socks = dict(poller.poll())
                except zmq.ZMQError:
                    # Die gracefully if the user sends a SIGQUIT
                    self._kill()
                    break

                if pull_socket in socks and socks[pull_socket] == zmq.POLLIN:
                    msg = self.pull_socket.recv()

                    if msg:
                        command, data = msg.split(self.delim)
                        self.process_command(command, data)

                if control_socket in socks and socks[control_socket] == zmq.POLLIN:
                    msg = self.control_socket.recv()

                    if msg:
                        worker, command, data = msg.split(self.delim)
                        self.process_command(command, data)
            else:
                msg = self.control_socket.recv()

                if msg:
                    worker, command, data = msg.split(self.delim)
                    self.process_command(command, data)

    def register(self):
        '''
        Register the node with the server.
        '''
        self.send_command('connect', self.worker_id)

    def send_command(self, command, data=None):
        '''
        Push a command to the sever.
        '''
        _d = self.delim

        if data:
            pdata = pickle.dumps(data)
            self.push_socket.send(cat(command,_d,pdata))
            #logging.debug(command)
        else:
            self.push_socket.send(cat(command,_d))
            #logging.debug(command)

    def set_bytecode(self, command, data):
        '''
        Load the bytecode sent by the server and flag that we are
        ready for work.
        '''
        #self.logging.info('Received Bytecode')
        mapfn_bc, reducefn_bc = data

        self.mapfn = types.FunctionType(
            marshal.loads(mapfn_bc),
            globals(),
            'mapfn'
        )
        self.reducefn = types.FunctionType(
            marshal.loads(reducefn_bc),
            globals(),
            'reducefn'
        )

        self.have_bytecode = True

    def on_done(self, command=None, data=None):
        #self.logging.info('Done')
        self._kill()

    def call_mapfn(self, command, data):
        results = defaultdict(list)
        key, value = data

        for k, v in self.mapfn(key, value):
            results[k].append(v)

        self.send_command('mapdone', (key, results))

    def call_reducefn(self, command, data):
        key, value = data
        results = self.reducefn(key, value)
        self.send_command('reducedone', (key, results))

    def process_command(self, command, data=None):
        commands = {
            'bytecode': self.set_bytecode,
            'done': self.on_done,
            'map': self.call_mapfn,
            'reduce': self.call_reducefn,
        }

        if command in commands:
            if data:
                data = pickle.loads(data)
            commands[command](command, data)

if __name__ == "__main__":
    c = Client()
    c.connect()
    c.start()
