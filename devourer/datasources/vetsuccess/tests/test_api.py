from devourer.core import data_publish
from devourer.datasources.vetsuccess import db


async def test_import_run(aiohttp_app, aiohttp_client, monkeypatch):
    log = []

    class FakePublisher:

        def publish(self, msg):
            log.append(('publish', msg))

    class DB:

        async def get_updates(self):
            for i in (1, 2, 3):
                yield ('test_table', i)

    async def connect(redis):
        return DB()

    monkeypatch.setattr(db, 'connect', connect)
    monkeypatch.setattr(data_publish, 'DataPublisher', FakePublisher)

    client = await aiohttp_client(aiohttp_app)
    resp = await client.get('/api/v1/vetsuccess/import')

    assert resp.status == 200
    assert await resp.text() == ''
    assert log == [
        ('publish', {'meta': {'data_source': 'vetsuccess', 'table_name': 'test_table'}, 'data': 1}),
        ('publish', {'meta': {'data_source': 'vetsuccess', 'table_name': 'test_table'}, 'data': 2}),
        ('publish', {'meta': {'data_source': 'vetsuccess', 'table_name': 'test_table'}, 'data': 3}),
    ]
