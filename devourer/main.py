import os
import aioredis
from aiohttp import web

from . import config
from .utils import log, module_loading
from .core.datasource import exceptions


async def create_redis_pool() -> aioredis.ConnectionsPool:
    return await aioredis.create_redis_pool(
        (config.REDIS_HOST, config.REDIS_PORT)
        # db=config.REDIS_DB
    )


async def on_startup(app: web.Application):
    app['redis_pool'] = await create_redis_pool()


async def on_shutdown(app: web.Application):
    app['redis_pool'].close()
    await app['redis_pool'].wait_closed()


def init_datasources(app: web.Application, url_prefix: str):
    for url_name, name in config.DATA_SOURCES:
        try:
            module = module_loading.import_string(f"devourer.datasources.{name}.setup.DataSourceSetup")
        except ImportError as e:
            raise exceptions.NoDataSourceFound(f"Data source `{name}` plugin not found") from e
        else:
            module(app, os.path.join(url_prefix, url_name))()


async def healthcheck(request):
    return web.Response(status=200)


async def get_application() -> web.Application:
    log.configure_logging(config.LOGGING_CONFIG, config.LOGGING)

    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    init_datasources(app, '/api/v1')

    app.add_routes([
        web.get('/', healthcheck),
    ])

    return app
