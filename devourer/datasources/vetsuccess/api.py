from aiohttp import web

from . import db


class VetSuccessImportRun(web.View):

    async def get(self) -> web.Response:
        conn = await db.connect(self.request.app['redis_pool'])

        # data = {}
        async for table_name, record in conn.get_updates():
            # if table_name not in data:
            #     data[table_name] = []
            # data[table_name].append(record)
            ...  # process new data

        return web.Response(
            content_type='application/json',
            status=200
        )
