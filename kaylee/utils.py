import time

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

import UserDict
import redis
import cPickle

class RedisHashSet(UserDict.DictMixin):
    def __init__(self):
        self._client = redis.Redis(host='localhost', port=6379, db=0)

    def keys(self, pattern="*"):
        return self._client.keys(pattern)

    def __len__(self):
        return self._client.dbsize()

    def __getitem__(self, key):
        return [cPickle.loads(el) for el in self._client.smembers(key)]

    def __setitem__(self, key, val):
        return self._client.sadd(key, cPickle.dumps(val))

    def __delitem__(self, key):
        return self._client.srem(key)

    def __contains__(self, key):
        return self._client.exists(key)

    def get(self, key, default=None):
        return self.__getitem__(key) or default
