from devourer.datasources.vetsuccess import db


async def test_import_run(aiohttp_app, aiohttp_client, monkeypatch):
    class DB:

        async def get_updates(self):
            print('!'*80)
            for i in (1, 2, 3):
                yield ('test_table', i)

    async def connect(redis):
        return DB()

    monkeypatch.setattr(db, 'connect', connect)

    client = await aiohttp_client(aiohttp_app)
    resp = await client.get('/api/v1/vetsuccess/import')

    assert resp.status == 200
    assert await resp.text() == ''
