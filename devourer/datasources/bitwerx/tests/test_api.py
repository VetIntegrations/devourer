import asyncio
import aioredis
import datetime
import pytest
from unittest import mock
from asynctest import CoroutineMock
from aiohttp import ClientSession

from devourer import config
from devourer.main import get_application
from devourer.core import data_publish
from devourer.utils import secret_manager
from devourer.datasources.bitwerx import api
from ..api import (
    check_status, get_download_response_status,
    set_last_updated_date, get_last_updated_date
)


async def test_check_status():
    # check_status process the response while we don't get 'Complete' status
    # and our test iterators will be empty after call check_status
    # so need to add one more 'Complete' and 'data' items to test iterators
    # that again call json() for check_status result

    statuses = iter(['Progressing'] + ['Complete'] * 2)
    results = iter([''] + ['data'] * 2)

    async def json_method():
        return {
            'status': next(statuses),
            'result': next(results)
        }

    log = []

    async def session_get(url, auth=None):
        m_json = mock.Mock(json=json_method, status=200)
        log.append('call get')
        return m_json

    m_session = mock.Mock(get=session_get)

    resp = await check_status(m_session, mock.Mock(headers={'Location': ''}), None, delay=0.1)

    assert await resp.json() == {'result': 'data', 'status': 'Complete'}
    assert len(log) == 2


async def test_get_download_response_status(monkeypatch):
    from devourer.datasources.bitwerx import api

    async def fake_check_status(session, response, auth):
        await asyncio.sleep(2)
        return 'success_data'

    monkeypatch.setattr(api, 'check_status', fake_check_status)

    result = await get_download_response_status(None, None, None)
    assert result == (True, 'success_data')

    monkeypatch.setattr(config, 'BITWERX_TIMEOUT', 1)
    result = await get_download_response_status(None, None, None)
    assert result == (None, None)


async def test_get_last_updated_date(monkeypatch):
    test_updated_date = '2020-07-27T00:00:00.0000000Z'
    practice_id = '1'

    m_redis = CoroutineMock()
    m_redis.get = CoroutineMock(return_value=test_updated_date.encode('utf-8'))
    monkeypatch.setattr(aioredis, "ConnectionsPool", m_redis)

    result = await get_last_updated_date(m_redis, practice_id)
    m_redis.get.assert_awaited_once_with(f'devourer.datasource.bitwerx.practice-{practice_id}')
    assert result == test_updated_date

    # if last_updated_date is absent in redis than we get '0001-01-01T00:00:00.0000000Z'
    m_redis.get = CoroutineMock(return_value=''.encode('utf-8'))
    result = await get_last_updated_date(m_redis, practice_id)
    assert result == '0001-01-01T00:00:00.0000000Z'


async def test_set_last_updated_date(monkeypatch):
    practice_id = '1'

    m_redis = CoroutineMock()
    m_redis.set = CoroutineMock()
    monkeypatch.setattr(aioredis, "ConnectionsPool", m_redis)

    test_update_date = datetime.datetime(1, 1, 1, 0, 0)

    await set_last_updated_date(m_redis, practice_id, test_update_date)

    m_redis.set.assert_awaited_once_with(
        f'devourer.datasource.bitwerx.practice-{practice_id}',
        test_update_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
    )


class TestImportRun:

    class FakeResponse:
        def __init__(self, status):
            self.status = status

    @pytest.fixture
    async def bitwerx_client(self, monkeypatch, aiohttp_client):

        class FakeSecretManger:

            def __init__(self, project):
                ...

            def get_secret(self, name):
                return {'bitwerx': {'username': 'test_vis', 'password': 'test_password'}}

        monkeypatch.setattr(
            config,
            'CUSTOMERS',
            {
                'test-customer': {
                    'datasources': ('bitwerx',),
                }
            }
        )

        monkeypatch.setattr(secret_manager, 'SecretManager', FakeSecretManger)

        app = await get_application()
        return await aiohttp_client(app)

    async def test_first_request_not_successful(self, bitwerx_client, monkeypatch):
        # if status is not 202 then we get 400 anyway

        monkeypatch.setattr(ClientSession, 'post', CoroutineMock(return_value=self.FakeResponse(status=400)))
        resp = await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')
        assert resp.status == 400

        monkeypatch.setattr(ClientSession, 'post', CoroutineMock(return_value=self.FakeResponse(status=500)))
        resp = await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')
        assert resp.status == 400

    async def test_first_request_is_successful_and_get_download_response_status_not(self, bitwerx_client, monkeypatch):
        monkeypatch.setattr(ClientSession, 'post', CoroutineMock(return_value=self.FakeResponse(status=202)))

        mock_download_response_status = CoroutineMock(return_value=(None, None))
        monkeypatch.setattr(api, 'get_download_response_status', mock_download_response_status)

        resp = await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')
        mock_download_response_status.assert_awaited_once()
        assert resp.status == 408

    async def test_first_request_is_successful_and_get_download_response_status_404(self, bitwerx_client, monkeypatch):
        monkeypatch.setattr(ClientSession, 'post', CoroutineMock(return_value=self.FakeResponse(status=202)))

        mock_download_response_status = CoroutineMock(return_value=(True, self.FakeResponse(status=404)))
        monkeypatch.setattr(api, 'get_download_response_status', mock_download_response_status)

        resp = await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')
        mock_download_response_status.assert_awaited_once()
        assert resp.status == 404

    async def test_all_requests_are_successful(self, bitwerx_client, monkeypatch):
        monkeypatch.setattr(data_publish, 'DataPublisher', mock.Mock())
        monkeypatch.setattr(ClientSession, 'post', CoroutineMock(return_value=self.FakeResponse(status=202)))

        mock_download_response_status = CoroutineMock(return_value=(True, self.FakeResponse(status=200)))
        monkeypatch.setattr(api, 'get_download_response_status', mock_download_response_status)

        mock_get_data = CoroutineMock(return_value={})
        monkeypatch.setattr(api, 'get_data', mock_get_data)

        resp = await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')
        mock_download_response_status.assert_awaited_once()
        mock_get_data.assert_awaited_once()
        assert resp.status == 200

    async def test_update_last_updated_date(self, bitwerx_client, monkeypatch):
        mock_get_last_updated_date = CoroutineMock(return_value='0001-01-01T00:00:00.0000000Z')
        mock_set_last_updated_date = CoroutineMock()

        monkeypatch.setattr(ClientSession, 'post', CoroutineMock(return_value=self.FakeResponse(status=400)))
        monkeypatch.setattr(api, 'get_last_updated_date', mock_get_last_updated_date)
        monkeypatch.setattr(api, 'set_last_updated_date', mock_set_last_updated_date)
        monkeypatch.setattr(data_publish, 'DataPublisher', mock.Mock())

        await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')

        mock_get_last_updated_date.assert_awaited_once()
        mock_set_last_updated_date.assert_not_awaited()

        mock_get_last_updated_date.reset_mock()
        mock_set_last_updated_date.reset_mock()

        monkeypatch.setattr(ClientSession, 'post', CoroutineMock(return_value=self.FakeResponse(status=202)))
        monkeypatch.setattr(
            api, 'get_download_response_status', CoroutineMock(return_value=(True, self.FakeResponse(status=200)))
        )
        monkeypatch.setattr(api, 'get_data', CoroutineMock(return_value={}))

        await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')

        mock_get_last_updated_date.assert_awaited_once()
        mock_set_last_updated_date.assert_awaited_once()

    async def test_data_publish(self, bitwerx_client, monkeypatch):

        log = []

        class FakePublisher:
            def publish(self, msg):
                log.append(('publish', msg))

            def exit(self):
                log.append('publisher.exit')

            def wait(self):
                log.append('publisher.wait')

        test_updated_date_1 = '2020-07-29T06:00:00.0000000'
        test_updated_date_2 = '2020-07-29T07:00:00.0000000'

        mock_download_response_status = CoroutineMock(return_value=(True, self.FakeResponse(status=200)))
        mock_get_data = CoroutineMock(
            return_value=[
                {'ID': 1, 'updated': test_updated_date_1},
                {'ID': 2, 'updated': test_updated_date_2}
            ]
        )

        monkeypatch.setattr(data_publish, 'DataPublisher', FakePublisher)
        monkeypatch.setattr(ClientSession, 'post', CoroutineMock(return_value=self.FakeResponse(status=202)))
        monkeypatch.setattr(api, 'get_download_response_status', mock_download_response_status)
        monkeypatch.setattr(api, 'get_data', mock_get_data)

        monkeypatch.setattr(api, 'validate_line_item', mock.Mock(return_value=False))
        resp = await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')
        mock_download_response_status.assert_awaited_once()
        mock_get_data.assert_awaited_once()
        assert resp.status == 422

        monkeypatch.setattr(api, 'validate_line_item', mock.Mock(return_value=True))
        resp = await bitwerx_client.get('/api/v1/import/test-customer/bitwerx/')
        assert resp.status == 200

        assert log == [
            (
                'publish',
                {
                    'meta': {'customer': 'test-customer', 'data_source': 'bitwerx', 'table_name': 'lineitem'},
                    'data': {'ID': 1, 'updated': test_updated_date_1},
                }
            ),
            (
                'publish',
                {
                    'meta': {'customer': 'test-customer', 'data_source': 'bitwerx', 'table_name': 'lineitem'},
                    'data': {'ID': 2, 'updated': test_updated_date_2},
                }
            ),
        ]
