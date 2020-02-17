import json
import aioredis
from aiohttp import web

from devourer import config


async def redis_delete(request):
    db_number = 0
    if request.query.get('n'):
        try:
            db_number = int(request.query.get('n'))
        except ValueError:
            pass

    if request.query.get('keys-pattern'):
        pattern = '{}*'.format(request.query.get('keys-pattern'))
        redis_conn = await aioredis.create_redis_pool(
            (config.REDIS_HOST, config.REDIS_PORT),
            db=int(db_number)
        )
        keys = await redis_conn.keys(pattern)
        if len(keys) > 0:
            cnt = await redis_conn.delete(*keys)
        else:
            return web.Response(status=404)

        return web.Response(status=200, body=str(cnt))

    return web.Response(status=400)


async def redis_hmset(request):
    params = await request.post()
    if 'n' in params and 'key' in params and 'data' in params:
        redis_cli = await aioredis.create_redis_pool(
            (config.REDIS_HOST, config.REDIS_PORT),
            db=int(params['n'])
        )
        await redis_cli.hmset_dict(params['key'], json.load(params['data'].file))

        return web.Response(status=200)
    return web.Response(status=400)


async def redis_set(request):
    params = await request.post()
    if 'n' in params and 'key' in params and 'value' in params:
        redis_cli = await aioredis.create_redis_pool(
            (config.REDIS_HOST, config.REDIS_PORT),
            db=int(params['n'])
        )
        await redis_cli.set(params['key'], params['value'])

        return web.Response(status=200)
    return web.Response(status=400)


async def idsmapping_rename(request):
    redis_cli = await aioredis.create_redis_pool(
        (config.REDIS_HOST, config.REDIS_PORT),
        db=1
    )
    ret = await redis_cli.rename('idsmapping-rarebreed-location', 'idsmapping-rarebreed-business')
    return web.Response(status=200, body=ret)


async def redis_checksum_storage_migrate(request):
    redis_conn = request.app['redis_pool']
    buckets = {
        key.decode('utf-8').split('-')[-2]
        for key in await redis_conn.keys('devourer.datasource.versuccess.checksums*')
    }
    for bucket in buckets:
        for key in await redis_conn.keys(f'devourer.datasource.versuccess.checksums-{bucket}-*'):
            await redis_conn.hmset_dict(
                f'devourer.datasource.versuccess.checksums-{bucket}',
                await redis_conn.hgetall(key)
            )
            await redis_conn.delete(key)

    return web.Response(status=200)
