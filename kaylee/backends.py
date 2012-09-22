#from redis import Redis
from collections import MutableMapping

class RedisShuffler(MutableMapping):
    conn = None

    def __init__(self):
        self.conn = self.conn or Redis(host='localhost', port=6379, db=1)

    def __getitem__(self, key):
        return self.conn.smembers(key)

    def __setitem__(self, key, value):
        return self.conn.sadd(key, value)

    def __len__(self):
        return int(self._client.dbsize())

    def __iter__(self):
        return self._client.keys("*")
