import pytest

from devourer.datasources.vetsuccess import db


async def test_store_new_ts_on_exit(monkeypatch):
    log = []

    class FakeRedis:

        async def set(self, key, value):
            log.append(('set', key, value))

    monkeypatch.setattr(db.time, 'time', lambda: 1234)

    assert len(log) == 0
    async with db.TimestampStorage('test', FakeRedis()):
        ...

    assert log == [('set', 'devourer.datasource.versuccess.timestamp-test', 1234)]


@pytest.mark.parametrize(
    'tablename, timestamp, expected',
    (
        ('test', 123, 123),
        ('testing', None, 0),
    )
)
async def test_get_latest(tablename, timestamp, expected):
    log = []

    class FakeRedis:

        async def get(self, key):
            log.append(('get', key))
            return timestamp

    stor = db.TimestampStorage(tablename, FakeRedis())
    assert await stor.get_latest() == expected
    assert log == [('get', f'devourer.datasource.versuccess.timestamp-{tablename}')]
