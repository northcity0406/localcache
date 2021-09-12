import string
from concurrent.futures import ThreadPoolExecutor
import time
import random

from cache.cache import Cache

value = ""
for i in range(10):
    value += "%d" % (random.randint(1, 1000))

print(value)


def get_random_str(length):
    return ''.join(random.sample(string.ascii_letters + string.digits, length))


@Cache(cache_time=50, use_redis=False)
def get_data(name):
    return {"data": name}


for i in range(200):
    value = get_random_str(random.randint(10, 20))
    print(get_data(value))
    print(get_data(value))
    print()
