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
    assert redis_log[0][1] == 'devourer.datasource.versuccess.checksums-test'
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


def test_set_log_update():
    stor = db.ChecksumStorage('test', None)

    updates = {1: 'a', 9: 'i'}

    assert stor.updated == {}
    for i, (key, value) in enumerate(updates.items()):
        stor[key] = value
        assert len(stor.updated) == i + 1

    assert stor.updated == updates
