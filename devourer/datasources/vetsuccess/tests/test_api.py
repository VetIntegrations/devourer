from devourer import config
from devourer.main import get_application
from devourer.core import data_publish
from devourer.datasources.vetsuccess import db


async def test_import_run(aiohttp_client, monkeypatch):
    log = []

    class FakePublisher:

        def publish(self, msg):
            log.append(('publish', msg))

    class DB:

        async def get_updates(self):
            for i in (1, 2, 3):
                yield ('test_table', i)

    async def connect(dsn, redis):
        return DB()

    monkeypatch.setattr(
        config,
        'CUSTOMERS',
        {
            'test-customer': {
                'datasources': {
                    'vetsuccess': {
                        'redshift_dsn': 'test-dsn',
                    }
                },
            }
        }
    )
    monkeypatch.setattr(db, 'connect', connect)
    monkeypatch.setattr(data_publish, 'DataPublisher', FakePublisher)

    app = await get_application()
    client = await aiohttp_client(app)
    resp = await client.get('/api/v1/import/test-customer/vetsuccess/')

    assert resp.status == 200
    assert await resp.text() == ''
    assert log == [
        (
            'publish',
            {
                'meta': {'customer': 'test-customer', 'data_source': 'vetsuccess', 'table_name': 'test_table'},
                'data': 1,
            },
        ),
        (
            'publish',
            {
                'meta': {'customer': 'test-customer', 'data_source': 'vetsuccess', 'table_name': 'test_table'},
                'data': 2,
            },
        ),
        (
            'publish',
            {
                'meta': {'customer': 'test-customer', 'data_source': 'vetsuccess', 'table_name': 'test_table'},
                'data': 3,
            },
        ),
    ]
