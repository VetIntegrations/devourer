import asyncio
from aiohttp import web

from devourer.core import data_publish
from . import db


class VetSuccessImportRun(web.View):

    async def get(self) -> web.Response:
        conn = await db.connect(self.request.app['redis_pool'])
        publisher = data_publish.DataPublisher()

        loop = asyncio.get_event_loop()
        async for table_name, record in conn.get_updates():
            await loop.run_in_executor(
                None,
                publisher.publish,
                {
                    'meta': {
                        'data_source': 'vetsuccess',
                        'table_name': table_name,
                    },
                    'data': record,
                }
            )

        return web.Response(
            content_type='application/json',
            status=200
        )
