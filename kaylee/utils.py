import time
import msgpack

def cat(*xs):
    return "".join(xs)

def print_timing(func):
  def wrapper(*arg):
    t1 = time.time()
    res = func(*arg)
    t2 = time.time()
    print '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0)
    return res
  return wrapper

def sub_subscription_prefix(worker_id, n=3):
    """
    Listen for n-tuples with the worker id prefix without
    deserialization. Very fast.
    """
    return msgpack.dumps(tuple([worker_id] + [None]*(n-1)))[0:2]
