import random
import marshal
import cPickle as pickle
import logging
import gevent
from gevent_zeromq import zmq
from utils import cat
from collections import defaultdict

START    = 0
MAP      = 1
REDUCE   = 2
FINISHED = 3

class Server:

    def __init__(self):

        self.workers = set()
        self.state = START

        self.mapfn = None
        self.reducefn = None
        self.data = None
        self.bytecode = None

        self.started = False
        self.completed = False
        self.delim = '::'

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

            # Pull tasks across manager
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

            print addr

            self.control_socket = c.socket(zmq.PUB)
            self.control_socket.bind(addr)


    def start(self, timeout=None):

        self.started = True
        self.logging.info('Started Server')

        try:
            if timeout:
                timeout = gevent.Timeout(timeout, gevent.Timeout)

            self.start_new_task()
            # Block until we collect all data
            gevent.spawn(self.collect).join(timeout=timeout)

        except KeyboardInterrupt:
            self.started = False
            self.logging.info('Stopped Server')

        except gevent.Timeout:
            self.started = False
            self.logging.info('Timed out')

        self.done()

    def done(self):
        for worker in self.workers:
            self.send_control('done',None,worker)

    def _kill(self):
        gevent.getcurrent().kill()

    def collect(self):
        while True:
            msg = self.pull_socket.recv()

            if msg:
                command, data = msg.split(self.delim)
                self.process_command(command, data)

    def results(self):
        if self.completed:
            return self.reduce_results
        else:
            return None

    #@print_timing
    def send_control(self, command, data, worker):
        _d = self.delim
        self.logging.debug('Sending to: %s' % worker)
        if data:
            pdata = pickle.dumps(data)
            #logging.debug( "<- %s" % command)
            self.control_socket.send(cat(worker,_d,command,_d,pdata))
        else:
            #logging.debug( "<- %s" % command)
            self.control_socket.send(cat(worker,_d,command ,_d))

    #@print_timing
    def send_command(self, command, data=None):
        _d = self.delim
        if data:
            pdata = pickle.dumps(data)
            #logging.debug( "<- %s" % command)
            self.push_socket.send(cat(command,_d, pdata))
        else:
            #logging.debug( "<- %s" % command)
            self.push_socket.send(cat(command ,_d))

    def start_new_task(self):
        command, data = self.next_task()
        if command:
            self.send_command(command, data)
            #gevent.spawn(self.send_command, command, data)

    def next_task(self):

        if self.state == START:

            self.map_iter = iter(self.data)
            self.working_maps = {}
            self.map_results = defaultdict(list)
            self.state = MAP
            self.logging.info('Mapping')

        if self.state == MAP:
            try:

                map_key = self.map_iter.next()
                map_item = map_key, self.data[map_key]
                self.working_maps[map_item[0]] = map_item[1]
                return 'map', map_item

            except StopIteration:
                if len(self.working_maps) > 0:
                    key = random.choice(self.working_maps.keys())
                    return 'map', (key, self.working_maps[key])
                self.state = REDUCE
                self.reduce_iter = self.map_results.iteritems()
                self.working_reduces = {}
                self.reduce_results = {}
                self.logging.info('Reducing')

        if self.state == REDUCE:
            try:

                reduce_item = self.reduce_iter.next()
                self.working_reduces[reduce_item[0]] = reduce_item[1]
                return 'reduce', reduce_item

            except StopIteration:

                if len(self.working_reduces) > 0:
                    key = random.choice(self.working_reduces.keys())
                    return 'reduce', (key, self.working_reduces[key])

                self.state = FINISHED

        if self.state == FINISHED:
            self.completed = True
            # Destroy the collector thread
            self._kill()

    def map_done(self, data):
        # Don't use the results if they've already been counted
        key, value = data
        if key not in self.working_maps:
            return

        for k, v in value.iteritems():
            self.map_results[k].extend(v)

        del self.working_maps[key]

    def reduce_done(self, data):
        # Don't use the results if they've already been counted
        key, value = data
        if key not in self.working_reduces:
            return

        self.reduce_results[key] = value
        del self.working_reduces[key]

    def on_map_done(self, command, data):
        self.map_done(data)
        self.start_new_task()

    def on_reduce_done(self, command, data):
        self.reduce_done(data)
        self.start_new_task()

    def gen_bytecode(self):
        self.bytecode = (
            marshal.dumps(self.mapfn.func_code),
            marshal.dumps(self.reducefn.func_code),
        )

    def on_connect(self, command, data):
        self.logging.info('Worker Registered: %s' % data)
        self.workers.add(data)
        worker_id = data

        # Store this so we don't call it for every worker
        if not self.bytecode:
            self.gen_bytecode()

        self.send_control(
            'bytecode',
            self.bytecode,
            worker_id
        )

        self.logging.info('Sending Bytecode')
        self.start_new_task()

    def process_command(self, command, data=None):
        commands = {
            'mapdone': self.on_map_done,
            'reducedone': self.on_reduce_done,
            'connect': self.on_connect
        }

        if command in commands:
            if data:
                data = pickle.loads(data)
            commands[command](command, data)
        else:
            self.process_command(self, command, data)
