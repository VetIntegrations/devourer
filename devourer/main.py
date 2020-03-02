import os
import aioredis
import sentry_sdk
from aiohttp import web
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from . import config
from .utils import log, module_loading, secret_manager
from .core.datasource import exceptions
from .views import devtools


async def create_redis_pool() -> aioredis.ConnectionsPool:
    return await aioredis.create_redis_pool(
        (config.REDIS_HOST, config.REDIS_PORT)
        # db=config.REDIS_DB
    )


async def on_startup(app: web.Application):
    app['secretmanager'] = secret_manager.SecretManager(config.GCP_PROJECT_ID)
    app['redis_pool'] = await create_redis_pool()


async def on_shutdown(app: web.Application):
    app['redis_pool'].close()
    await app['redis_pool'].wait_closed()


def init_customers(app: web.Application, url_prefix: str):
    for customer, options in config.CUSTOMERS.items():
        customer_url_prefix = os.path.join(url_prefix, customer)
        for source_name in options['datasources']:
            try:
                module = module_loading.import_string(f'devourer.datasources.{source_name}.setup.DataSourceSetup')
            except ImportError as ex:
                raise exceptions.NoDataSourceFound(f"Data source `{source_name}` plugin not found") from ex
            else:
                module(app, os.path.join(customer_url_prefix, source_name))(customer)


async def healthcheck(request):
    return web.Response(status=200)


async def get_application() -> web.Application:
    log.configure_logging(config.LOGGING_CONFIG, config.LOGGING)

    sentry_sdk.init(
        dsn=config.SENTRY_DSN,
        integrations=[AioHttpIntegration()]
    )

    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    init_customers(app, '/api/v1/import')

    app.add_routes([
        web.get('/', healthcheck),
    ])

    if config.DEBUG:
        app.add_routes([
            web.get('/admin/redis/delete/', devtools.redis_delete),
            web.post('/admin/redis/set/', devtools.redis_set),
            web.post('/admin/redis/hmset/', devtools.redis_hmset),
            web.post('/admin/redis/storage-migrate/', devtools.redis_checksum_storage_migrate),
            web.post('/admin/redis/idsmapping/rename/', devtools.idsmapping_rename),
        ])

    return app
