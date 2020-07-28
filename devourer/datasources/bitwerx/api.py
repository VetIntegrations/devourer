import datetime
import asyncio
import gzip
import json
import logging
from aiohttp import web, ClientSession, BasicAuth

from devourer import config

logger = logging.getLogger('devourer.datasource.bitwerx')

format_timestamp = '%Y-%m-%dT%H:%M:%S.%f'


async def check_status(session, response, auth, delay=10):
    while True:
        await asyncio.sleep(delay)
        dw_resp = await session.get(response.headers['Location'], auth=auth)
        if dw_resp.status == 200:
            resp_data = await dw_resp.json()
            if resp_data['status'] == 'Complete':
                break

    return dw_resp


async def get_data(session, response):
    resp_data = await response.json()
    file_resp = await session.get(resp_data['downloadUrl'])
    file_data = await file_resp.content.read(n=-1)
    return json.loads(gzip.decompress(file_data))


async def get_download_response_status(session, response, auth):
    ok = dw_resp = None
    try:
        dw_resp = await asyncio.wait_for(
            check_status(session, response, auth),
            timeout=config.BITWERX_TIMEOUT
        )
        ok = True
    except asyncio.TimeoutError:
        ...

    return ok, dw_resp


def get_redis_key(practice_id):
    return 'devourer.datasource.bitwerx.practice-{}'.format(practice_id)


async def get_last_updated_date(redis, practice_id):
    last_updated_date = await redis.get(get_redis_key(practice_id))

    if last_updated_date:
        last_updated_date = last_updated_date.decode('utf-8')
    else:
        last_updated_date = '0001-01-01T00:00:00.0000000Z'

    return last_updated_date


async def set_last_updated_date(redis, practice_id, updated_date):
    await redis.set(get_redis_key(practice_id), updated_date.strftime(format_timestamp))


async def import_run(request, customer_name: str = None) -> web.Response:
    bw_config = request.app['secretmanager'].get_secret(customer_name)['bitwerx']
    username = bw_config['username']
    password = bw_config['password']

    practice_id = '1234|1'

    url = 'https://partner.daylight.vet/api/downloadRequest'

    redis = request.app['redis_pool']

    payload = {
        'practiceId': practice_id,
        'lastUpdatedDateUtc': await get_last_updated_date(redis, practice_id),
        'recordType': 'lineItem',
    }

    session = ClientSession()
    auth = BasicAuth(username, password)

    response = await session.post(url=url, data=json.dumps(payload), auth=auth)

    web_response = web.Response(status=200)
    if response.status == 202:
        ok, response = await get_download_response_status(session, response, auth)
        if ok:
            if response.status == 200:
                data = await get_data(session, response)

                max_updated_date = datetime.datetime(1, 1, 1, 0, 0)
                for item in data:
                    updated_date = datetime.datetime.strptime(item['Updated'][:-1], format_timestamp)
                    max_updated_date = max(max_updated_date, updated_date)

                # after successfully publish
                await set_last_updated_date(redis, practice_id, max_updated_date)
            else:
                web_response = web.Response(status=404)
        else:
            web_response = web.Response(status=408)
    else:
        web_response = web.Response(status=400)

    # publish
    await session.close()
    logger.info(
        f'{customer_name}: Bitwerx data source, practiceId - {payload["practiceId"]},'
        f' status code - {web_response.status}'
    )

    return web_response
