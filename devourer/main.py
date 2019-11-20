import aioredis
from aiohttp import web

from . import config


async def create_redis_pool() -> aioredis.ConnectionsPool:
    return await aioredis.create_pool(
        (config.REDIS_HOST, config.REDIS_PORT),
        db=config.REDIS_DB
    )


async def on_startup(app: web.Application):
    app['redis_pool'] = await create_redis_pool()


async def on_shutdown(app: web.Application):
    await app['redis_pool'].clear()


async def get_application() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    return app
