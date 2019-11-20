import aioredis
from aiohttp import web

from . import config
from .utils import log


async def create_redis_pool() -> aioredis.ConnectionsPool:
    return await aioredis.create_pool(
        (config.REDIS_HOST, config.REDIS_PORT),
        db=config.REDIS_DB
    )


async def on_startup(app: web.Application):
    app['redis_pool'] = await create_redis_pool()


async def on_shutdown(app: web.Application):
    await app['redis_pool'].clear()


async def healthcheck(request):
    return web.Response(status=200)


async def get_application() -> web.Application:
    log.configure_logging(config.LOGGING_CONFIG, config.LOGGING)

    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    app.add_routes([
        web.get('/', healthcheck),
    ])

    return app
