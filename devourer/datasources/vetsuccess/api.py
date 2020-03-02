import asyncio
from aiohttp import web

from devourer.core import data_publish
from . import db


async def import_run(request, customer_name: str = None) -> web.Response:
    config = request.app['secretmanager'].get_secret(customer_name)['vetsuccess']

    conn = await db.connect(
        config['redshift_dsn'],
        request.app['redis_pool']
    )
    publisher = data_publish.DataPublisher()

    loop = asyncio.get_event_loop()
    async for table_name, record in conn.get_updates():
        await loop.run_in_executor(
            None,
            publisher.publish,
            {
                'meta': {
                    'customer': customer_name,
                    'data_source': 'vetsuccess',
                    'table_name': table_name,
                },
                'data': record,
            }
        )

    await conn.close()

    return web.Response(status=200)
