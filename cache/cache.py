import datetime
import json
import sys
import heapq
from readerwriterlock import rwlock

from database.redis_client import get_redis_conn

cache_data = {}
MAX_CACHE_DATA_SIZE = 5000
MAX_LOCAL_CACHE_EXPIRE_SECONDS = 1800
lock = rwlock.RWLockFair()
local_cache_heap = []


def get_local_cache_size():
    return sys.getsizeof(cache_data)


class Item(object):
    def __init__(self, key, value, expire_at=None):
        self.key = key
        self.value = value
        self.expire_at = expire_at


class Cache(object):
    def __init__(self, cache_time=10, use_redis=False):
        self._cache_time = min(cache_time, MAX_LOCAL_CACHE_EXPIRE_SECONDS)
        self._use_redis = use_redis

    def delete_local_cache(self, cache_key):
        del cache_data[cache_key]

    def clear_expired_local_cache(self):
        if len(cache_data) > 0:
            item = local_cache_heap[0]
            if item[0] <= datetime.datetime.now().timestamp():
                self.delete_local_cache(item[1])
                heapq.heappop(local_cache_heap)

    def get_local_cache(self, cache_key):
        print([cache_key, "get_local_cache"])
        r_lock = lock.gen_rlock()
        r_lock.acquire()
        item = cache_data.get(cache_key)
        if not item:
            r_lock.release()
            return None
        if item.expire_at <= datetime.datetime.now().timestamp():
            self.delete_local_cache(cache_key)
            r_lock.release()
            return None
        self.clear_expired_local_cache()
        r_lock.release()
        return item.value

    def set_local_cache(self, key, value):
        print([key, value, "set_local_cache"])
        w_lock = lock.gen_wlock()
        w_lock.acquire()
        if get_local_cache_size() >= MAX_CACHE_DATA_SIZE:
            w_lock.release()
            self.set_redis_cache(key, value)
            return
        expire_at = datetime.datetime.now().timestamp() + self._cache_time
        cache_data[key] = Item(key, value, expire_at=expire_at)
        heapq.heappush(local_cache_heap, (expire_at, key))
        w_lock.release()

    def set_redis_cache(self, key, value):
        print([key, value, "set_redis_cache"])
        if isinstance(value, (int, bool, float, dict, list, tuple, set)):
            value = json.dumps({"data": value})
        else:
            return
        conn = get_redis_conn()
        conn.set(key, value, ex=self._cache_time)
        conn.close()

    def get_redis_cache(self, cache_key):
        print([cache_key, "get_redis_cache"])
        conn = get_redis_conn()
        data = conn.get(cache_key)
        conn.close()
        if data:
            data = json.loads(data)
            return data['data']
        return None

    def generate_cache_key(self, func, *args, **kwargs):
        key = func.__name__ + "_" + "_".join([str(v) for v in args])
        key += "_".join(["%s_%s" % (k, v) for k, v in kwargs])
        return key

    def __call__(self, func):
        def wrap(*args, **kwargs):
            cache_key = self.generate_cache_key(func, *args, **kwargs)
            if self._use_redis:
                value = self.get_redis_cache(cache_key)
            else:
                value = self.get_local_cache(cache_key)
            if value:
                return value
            elif not self._use_redis:
                value = self.get_redis_cache(cache_key)
                if value:
                    return value
            value = func(*args, **kwargs)
            if self._use_redis:
                self.set_redis_cache(cache_key, value)
            else:
                self.set_local_cache(cache_key, value)
            return value

        return wrap
