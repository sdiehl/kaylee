import random
import marshal
import logging
import gevent

import zmq.green as zmq
from collections import defaultdict

START     = 0
MAP       = 1
SHUFFLE   = 2
PARTITION = 3
REDUCE    = 4
COLLECT   = 5

try:
    import msgpack as srl
except ImportError:
    import cPickle as srl

class Server(object):

    def __init__(self):

        self.workers = set()
        self.state = START

        self.mapfn = None
        self.reducefn = None
        self.datafn = None

        self.bytecode = None

        self.started = False
        self.completed = False

        self.working_maps = {}

        logging.basicConfig(logging=logging.DEBUG,
            format="%(asctime)s [%(levelname)s] %(message)s")
        logging.getLogger("").setLevel(logging.INFO)
        self.logging = logging

    def main_loop(self):
        self.started = True

        poller = zmq.Poller()

        poller.register(self.pull_socket, zmq.POLLIN)
        poller.register(self.push_socket, zmq.POLLOUT)
        poller.register(self.ctrl_socket, zmq.POLLOUT)

        while self.started and not self.completed:
            try:
                events = dict(poller.poll())
            except zmq.ZMQError:
                self._kill()
                break

            # Specify number of nodes to requeset
            if len(self.workers) > 0:
                if events.get(self.push_socket) == zmq.POLLOUT:
                    self.start_new_task()
                if events.get(self.ctrl_socket) == zmq.POLLIN:
                    self.manage()
                if events.get(self.pull_socket) == zmq.POLLIN:
                    self.collect_task()
            else:
                if events.get(self.pull_socket) == zmq.POLLIN:
                    self.collect_task()
                if events.get(self.ctrl_socket) == zmq.POLLIN:
                    self.manage()

    def connect(self, push_addr = None, pull_addr = None, control_addr = None):
        c = zmq.Context()

        # Pull tasks across manager
        if not pull_addr:
            prot = 'tcp://'
            ip   = '127.0.0.1'
            port = '6666'
            addr = ''.join([prot,ip,':',port])
        elif len(pull_addr) > 1:
            prot, ip, port = pull_addr
            addr = ''.join([prot,ip,':',port])
        else:
            addr = pull_addr

        print addr

        self.pull_socket = c.socket(zmq.PULL)
        self.pull_socket.bind(addr)

        if not push_addr:
            prot = 'tcp://'
            ip   = '127.0.0.1'
            port = '5555'
            addr = ''.join([prot,ip,':',port])
        elif len(push_addr) > 1:
            prot, ip, port = push_addr
            addr = ''.join([prot,ip,':',port])
        else:
            addr = push_addr

        print addr

        self.push_socket = c.socket(zmq.PUSH)
        self.push_socket.bind(addr)

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

        print 'Control Socket', addr

        self.ctrl_socket = c.socket(zmq.ROUTER)
        self.ctrl_socket.bind(addr)

    def start(self, timeout=None):
        self.gen_bytecode()
        self.logging.info('Started Server')

        main = gevent.spawn(self.main_loop)
        main.join()

        self.done()

    def done(self):
        for worker in self.workers:
            self.ctrl_socket.send_multipart([worker, 'done'])

    def _kill(self):
        gevent.getcurrent().kill()

    def results(self):
        if self.completed:
            return self.reduce_results
        else:
            return None

    def send_datum(self, command, key, data):
        self.push_socket.send(command, flags=zmq.SNDMORE)
        self.push_socket.send(str(key), flags=zmq.SNDMORE)
        # Do a multipart message since we want to do
        # zero-copy of data.

        if self.state == MAP:
            self.push_socket.send(data, copy=False)
        else:
            self.push_socket.send(srl.dumps(data))

    def send_command(self, command, payload=None):
        if payload:
            self.send_datum(command, *payload)
        else:
            self.push_socket.send(command)

    def start_new_task(self):
        action = self.next_task()
        if action:
            command, data = action
            self.send_command(command, data)

    def next_task(self):

        if self.state == START:

            #self.job_id = 'foo'
            self.map_iter = self.datafn()
            self.map_results = defaultdict(list)
            self.state = MAP
            self.logging.info('Mapping')

        if self.state == MAP:

            try:
                map_key, map_item = self.map_iter.next()
                self.working_maps[str(map_key)] = map_item
                #print 'sending', map_key
                return 'map', (map_key, map_item)
            except StopIteration:
                self.logging.info('Shuffling')
                self.state = SHUFFLE

        if self.state == SHUFFLE:
            self.reduce_iter = self.map_results.iteritems()
            self.working_reduces = set()
            self.reduce_results = {}

            if len(self.working_maps) == 0:
                self.logging.info('Reducing')
                self.state = PARTITION
            #else:
                #self.logging.info('Still shuffling %s ' % len(self.working_maps))

        if self.state == PARTITION:
            self.state = REDUCE

        if self.state == REDUCE:

            try:
                reduce_key, reduce_value = self.reduce_iter.next()
                self.working_reduces.add(reduce_key)
                return 'reduce', (reduce_key, reduce_value)
            except StopIteration:
                self.logging.info('Collecting')
                self.state = COLLECT

        if self.state == COLLECT:

            if len(self.working_reduces) == 0:
                self.completed = True
                self.logging.info('Finished')
            #else:
                #self.logging.info('Still collecting %s' % len(self.working_reduces))

    def collect_task(self):
        # Don't use the results if they've already been counted
        command = self.pull_socket.recv(flags=zmq.SNDMORE)

        if command == 'connect':
            payload = self.pull_socket.recv()
            self.on_connect(payload)

        elif command == 'mapkeydone':
            key = self.pull_socket.recv()
            del self.working_maps[key]

        elif command == 'mapdone':
            key = self.pull_socket.recv(flags=zmq.SNDMORE)
            tkey = self.pull_socket.recv(flags=zmq.SNDMORE)
            value = self.pull_socket.recv()

            #print tkey, key, value
            self.map_results[tkey].extend(value)

            #del self.working_maps[key]

        elif command == 'reducedone':
            key = self.pull_socket.recv(flags=zmq.SNDMORE)
            value = srl.loads(self.pull_socket.recv())

            # Don't use the results if they've already been counted
            if key not in self.working_reduces:
                return

            self.reduce_results[key] = value
            self.working_reduces.remove(key)

        else:
            raise RuntimeError()

    def on_map_done(self, command, data):
        self.map_done(data)

    def on_reduce_done(self, command, data):
        self.reduce_done(data)

    def gen_bytecode(self):
        self.bytecode = (
            marshal.dumps(self.mapfn.func_code),
            marshal.dumps(self.reducefn.func_code),
        )

    def on_connect(self, worker_id):
        if worker_id not in self.workers:
            self.logging.info('Worker Registered: %s' % worker_id)
            self.workers.add(worker_id)

            payload = ('bytecode', self.bytecode)
            self.ctrl_socket.send_multipart([worker_id, srl.dumps(payload)])
            self.logging.info('Sending Bytecode')
        else:
            print worker_id

    def process_command(self, command, data=None):
        self.commands[command](self, command, data)

    commands = {
        'mapdone'    : on_map_done,
        'reducedone' : on_reduce_done,
        'connect'    : on_connect
    }

if __name__ == '__main__':
    # Job submission

    # Support Cython!
    import sys
    import imp

    path = sys.argv[1]
    imp.load_module(path)
