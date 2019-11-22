import pytest

from devourer.datasources.vetsuccess import db


async def test_sync_on_exit():
    redis_log = []

    class FakeRedis:

        async def hmset_dict(self, key, _dict):
            redis_log.append(('hmset_dict', key, _dict, ))

    assert len(redis_log) == 0
    async with db.ChecksumStorage('test', FakeRedis()) as stor:
        stor[1] = 'a'
        stor[2] = 'b'

    assert len(redis_log) == 1
    assert redis_log[0][0] == 'hmset_dict'
    assert redis_log[0][1] == 'devourer.datasource.versuccess.checksums-test-0'
    assert redis_log[0][2] == {1: 'a', 2: 'b'}


async def test_clear_update_on_sync():

    class FakeRedis:

        async def hmset_dict(self, key, _dict):
            ...

    async with db.ChecksumStorage('test', FakeRedis()) as stor:
        stor[1] = 'a'
        stor[2] = 'b'
        assert len(stor.updated) == 2
        await stor.sync_current_block()
        assert len(stor.updated) == 0


async def test_sync_on_getting_new_block(monkeypatch):
    log = []

    class FakeRedis:

        async def hgetall(self, key, encoding):
            return {}

    async def sync_current_block(self):
        log.append('sync_current_block')

    monkeypatch.setattr(db.ChecksumStorage, 'sync_current_block', sync_current_block)

    stor = db.ChecksumStorage('test', FakeRedis(), block_size=10)
    assert len(log) == 0
    await stor[1]
    assert len(log) == 1
    await stor[5]
    assert len(log) == 1
    await stor[11]
    assert len(log) == 2
    assert log == ['sync_current_block', 'sync_current_block', ]


@pytest.mark.parametrize(
    'block_size, request_keys, expected_log',
    (
        (
            10,
            (
                (1, (1, 3, 5)),
                (2, (11, )),
                (3, (25, 29)),
            ),
            [
                ('redis-hgetall', 'devourer.datasource.versuccess.checksums-test-0', 'utf-8'),
                ('redis-hgetall', 'devourer.datasource.versuccess.checksums-test-1', 'utf-8'),
                ('redis-hgetall', 'devourer.datasource.versuccess.checksums-test-2', 'utf-8'),
            ],
        ),
        (
            100,
            (
                (1, (1, 30, 99)),
                (2, (100, 184)),
                (3, (201, 291)),
                (4, (39, )),
            ),
            [
                ('redis-hgetall', 'devourer.datasource.versuccess.checksums-test-0', 'utf-8'),
                ('redis-hgetall', 'devourer.datasource.versuccess.checksums-test-1', 'utf-8'),
                ('redis-hgetall', 'devourer.datasource.versuccess.checksums-test-2', 'utf-8'),
                ('redis-hgetall', 'devourer.datasource.versuccess.checksums-test-0', 'utf-8'),
            ],
        ),
    )
)
async def test_getting_blocks(block_size, request_keys, expected_log):
    log = []

    class FakeRedis:

        async def hgetall(self, key, encoding='utf-8'):
            log.append(('redis-hgetall', key, encoding))
            return {}

    stor = db.ChecksumStorage('test', FakeRedis(), block_size=block_size)
    for count, keys in request_keys:
        for key in keys:
            await stor[key]
        assert len(log) == count

    assert log == expected_log


def test_set_log_update():
    stor = db.ChecksumStorage('test', None, block_size=10)

    updates = {1: 'a', 9: 'i'}

    assert stor.updated == {}
    for i, (key, value) in enumerate(updates.items()):
        stor[key] = value
        assert len(stor.updated) == i + 1

    assert stor.updated == updates
