import hashlib
import json
from functools import wraps


def method_with_args_to_cache_key(fn, *args, **kwargs):
    hash_data = '{}-{}'.format(
        '.'.join([str(val) for val in args]),
        '.'.join('{}:{}'.format(str(arg), str(val)) for arg, val in kwargs.items())
    )
    hash = hashlib.sha1(hash_data.encode('utf-8'))

    return 'cache.{}:{}'.format(fn.__qualname__, hash.hexdigest())


def cached_method(timeout: int):
    def _cached_method(fn):
        @wraps(fn)
        def __cached_method(self, *args, **kwargs):
            key = method_with_args_to_cache_key(fn, *args, **kwargs)
            data = None
            cached = self.redis.get(key)
            if not cached:
                data = fn(self, *args, **kwargs)
                self.redis.set(key, json.dumps(data), ex=timeout)
            else:
                data = json.loads(cached)

            return data
        return __cached_method
    return _cached_method
