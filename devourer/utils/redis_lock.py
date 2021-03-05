import redis


class RedisLock(object):
    """
    Locking context object.

    Usage:

    ```
    with RedisLock('key_lock'):
        <your code>
    ````
    Will run your code when 'key_lock' is unlocked. During running your code 'key_lock' is locked.
    If 'key_lock' is locked - will wait when 'key_lock' became is unlocked and then will lock key and run your code.
    """

    def __init__(self, redis_conn, lock_key, timeout=60):
        self.lock_key = lock_key
        self.timeout = timeout
        self.lock = redis_conn.lock(lock_key, timeout)

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.lock.release()
        except redis.exceptions.LockError:
            pass
