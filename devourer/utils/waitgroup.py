from redis.client import Redis

from devourer.utils.redis_lock import RedisLock


class WaitGroupStopException(Exception):
    ...


class WaitGroup:
    def __init__(self, key: str, redis: Redis):
        self.key = key
        self.redis = redis
        self.lock_key = f'waitgroup_lock_{key}'

    def count(self):
        return int(self.redis.get(self.key) or 0)

    def add(self, n: int):
        with RedisLock(self.redis, self.lock_key):
            count = int(self.redis.get(self.key) or 0)
            count += n
            self.redis.set(self.key, count)

    def done(self):
        with RedisLock(self.redis, self.lock_key):
            count = int(self.redis.get(self.key))
            count -= 1
            self.redis.set(self.key, count)

    def stop(self):
        with RedisLock(self.redis, self.lock_key):
            self.redis.set(self.key, -1)


def waitgroup_mark(func):
    func.__waitgroup__ = True
    return func


def waitgroup_flow(func):
    def wrapper(self, *args, **kwargs):
        blocking_waitgroup_key, _ = kwargs['waitgroup_keys']
        count = WaitGroup(blocking_waitgroup_key, self.redis).count() if blocking_waitgroup_key else 0
        if count == 0:
            func(self, *args, **kwargs)
        elif count == -1:
            raise WaitGroupStopException
        else:
            self.apply_async(
                kwargs=kwargs,
                countdown=10
            )
    return wrapper
