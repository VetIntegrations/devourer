import hashlib
import pytest
from datetime import datetime
from unittest.mock import Mock, call

from .. import cache


@pytest.mark.parametrize(
    'fn, args, kwargs, expected',
    (
        (str, [], {}, 'cache.str:-'),
        (str, ['a'], {}, 'cache.str:a-'),
        (str, ['a', 1, None], {}, 'cache.str:a.1.None-'),
        (str, [1], {'now': datetime(2021, 1, 24, 18, 59)}, 'cache.str:1-now:2021-01-24 18:59:00'),
    )
)
def test_method_with_args_to_cache_key(fn, args, kwargs, expected):
    expected_key, hash_data = expected.split(':', 1)
    hash = hashlib.sha1(hash_data.encode('utf-8')).hexdigest()
    key = cache.method_with_args_to_cache_key(fn, *args, **kwargs)
    assert key == f'{expected_key}:{hash}'


def test_ceched(monkeypatch):
    m_redis = Mock()
    m_redis.get.side_effect = (None, '50')

    class CacheTested:

        def __init__(self, redis):
            self.redis = redis

        @cache.cached_method(5)
        def cached_method(self, *args, **kwargs):
            return 1

    tested = CacheTested(m_redis)
    assert tested.cached_method() == 1
    assert tested.cached_method() == 50

    cache_key = 'cache.test_ceched.<locals>.CacheTested.cached_method:3bc15c8aae3e4124dd409035f32ea2fd6835efc9'
    m_redis.assert_has_calls(
        (
            call.get(cache_key),
            call.set(cache_key, '1', ex=5),
            call.get(cache_key),
        ),
        any_order=False
    )
