import redis


def get_redis_conn():
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, socket_keepalive=True)
    return redis.Redis(connection_pool=pool)
