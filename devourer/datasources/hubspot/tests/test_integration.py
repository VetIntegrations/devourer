import copy
import logging
import json
import time
import pytest
from datetime import datetime
from unittest.mock import Mock

from devourer import config
from devourer.utils.constants import Integration, CONFIG_CUSTOMER_INTEGRATIONS_KEY
from .. import integration
from ..exception import HubSpotDatetimeFormatParseException
from ..integration import hubspot_datetime_parse, HubSpotFetchUpdates


@pytest.mark.parametrize(
    'input, output',
    (
        ('2018-03-28T17:54:35.463Z', datetime(2018, 3, 28, 17, 54, 35, 463000)),
        ('2018-03-28T17:54:00Z', datetime(2018, 3, 28, 17, 54, 0)),
    )
)
def test_hubspot_datetime_parse(input, output):
    assert hubspot_datetime_parse(input) == output


def test_hubspot_datetime_parse__bad_format():
    with pytest.raises(HubSpotDatetimeFormatParseException):
        hubspot_datetime_parse('2018-03-28T17:54Z')


class TestHubSpotFetchUpdates:

    @pytest.fixture
    def mock_customerconfig(self, monkeypatch):
        m_customer_config = Mock()
        m_customer_config.get_customer_config.return_value = {
            CONFIG_CUSTOMER_INTEGRATIONS_KEY: {
                Integration.HUBSPOT.value: {},
            },
        }
        monkeypatch.setattr(integration, 'CustomerConfig', Mock(return_value=m_customer_config))

        yield m_customer_config

    @pytest.fixture
    def mock_publisher_client(self, monkeypatch):
        m_publisherclient = Mock()

        monkeypatch.setattr(integration.pubsub_v1, 'PublisherClient', Mock(return_value=m_publisherclient))

        yield m_publisherclient

    def test_get_last_update_field(self, mock_customerconfig, mock_publisher_client):
        mock_customerconfig.get_customer_config.return_value = {
            CONFIG_CUSTOMER_INTEGRATIONS_KEY: {
                Integration.HUBSPOT.value: {
                    'objects': {
                        'companies': {'last_update_field': 'A'},
                        'deals': {'last_update_field': 'B'},
                    }
                }
            }
        }

        hs = HubSpotFetchUpdates('test', None, None)
        assert hs.get_last_update_field('companies') == 'A'
        assert hs.get_last_update_field('deals') == 'B'

        with pytest.raises(KeyError):
            hs.get_last_update_field('foobar')

    def test_get_api_request_params(self, mock_customerconfig, mock_publisher_client, monkeypatch):
        mock_customerconfig.get_customer_config.return_value = {
            CONFIG_CUSTOMER_INTEGRATIONS_KEY: {
                Integration.HUBSPOT.value: {
                    'objects': {
                        'companies': {
                            'properties': ('fld1', 'fld2'),
                        },
                        'deals': {
                            'properties': ('fldA', 'fldB', 'fldC'),
                        },
                    }
                }
            }
        }

        hs = HubSpotFetchUpdates('test', None, None)
        monkeypatch.setattr(hs, 'get_last_update_field', Mock(return_value='foo'))

        assert hs.get_api_request_params('companies', 500) == {
            'limit': 500,
            'properties': ('fld1', 'fld2'),
            'sorts': ['foo'],
        }

        assert hs.get_api_request_params('companies', 500, after='50') == {
            'limit': 500,
            'properties': ('fld1', 'fld2'),
            'sorts': ['foo'],
            'after': '50',
        }

        assert hs.get_api_request_params('companies', 500, last_update=b'50') == {
            'limit': 500,
            'properties': ('fld1', 'fld2'),
            'sorts': ['foo'],
            'filterGroups': {
                'filters': [
                    {
                        'value': 50 * 1000,
                        'propertyName': 'foo',
                        'operator': 'GT',
                    },
                ],
            }
        }

    def test_publish_object(self, mock_customerconfig, mock_publisher_client, monkeypatch):
        CUSTOMER_NAME = 'test-customer'
        OBJ_NAME = 'companies'
        hs = HubSpotFetchUpdates(CUSTOMER_NAME, None, None)
        m_future = Mock()
        m_publisher = Mock(**{'publish.return_value': m_future})
        monkeypatch.setattr(hs, 'publisher', m_publisher)

        item = {'foo': 'bar'}
        ret = hs.publish_object(OBJ_NAME, item)

        msg = {
            'meta': {
                'customer': CUSTOMER_NAME,
                'data_source': 'hubspot',
                'table_name': OBJ_NAME,
            },
            'data': item,
        }
        m_publisher.publish.assert_called_once_with(
            'projects/{project_id}/topics/{topic}'.format(
                project_id=config.GCP_PROJECT_ID,
                topic=config.GCP_PUBSUB_PUBLIC_TOPIC,
            ),
            json.dumps(msg).encode('utf-8')
        )
        assert ret == m_future

    def test_get_last_update(self, mock_customerconfig, mock_publisher_client):
        m_redis = Mock(**{'get.return_value': b'bar'})

        hs = HubSpotFetchUpdates('test', m_redis, None)
        assert hs.get_last_update('foo') == b'bar'
        m_redis.get.assert_called_once_with('last-update__{}_{}'.format('test', 'foo'))

    def test_set_last_update(self, mock_customerconfig, mock_publisher_client):
        m_redis = Mock()

        now = datetime.now()
        hs = HubSpotFetchUpdates('test', m_redis, None)
        hs.set_last_update('foo', now)
        now_timestamp = time.mktime(now.timetuple())
        m_redis.set.assert_called_once_with(
            'last-update__{}_{}'.format('test', 'foo'),
            int(now_timestamp)
        )

    def test_run_first_import_with_after(self, mock_customerconfig, mock_publisher_client, monkeypatch):
        APIKEY = 'r42hirr4hu3iort3o'
        RESP_AFTER = '43'
        RESULT1 = {'properties': {'last_update': '2018-03-28T17:54:00Z'}}
        RESULT2 = {'properties': {'last_update': '2018-03-29T17:54:00Z'}}
        REQUEST_PARAMS = {'fld1': 'val1'}

        mock_customerconfig.get_customer_config.return_value = {
            CONFIG_CUSTOMER_INTEGRATIONS_KEY: {
                Integration.HUBSPOT.value: {
                    'apikey': APIKEY,
                },
            },
        }

        m_redis = Mock(**{'get.return_value': b'bar'})
        m_task = Mock()
        m_requests = Mock()
        m_publish_future = Mock()
        m_publish_object = Mock(return_value=m_publish_future)
        m_set_last_update = Mock()
        hs = HubSpotFetchUpdates('test', m_redis, m_task)

        monkeypatch.setattr(integration, 'requests', m_requests)
        monkeypatch.setattr(hs, 'get_last_update', Mock(return_value=None))
        monkeypatch.setattr(hs, 'set_last_update', m_set_last_update)
        monkeypatch.setattr(hs, 'publish_object', m_publish_object)
        monkeypatch.setattr(hs, 'get_last_update_field', Mock(return_value='last_update'))
        monkeypatch.setattr(hs, 'get_api_request_params', Mock(return_value=REQUEST_PARAMS))

        m_response = Mock(**{
            'status_code': 200,
            'json.return_value': {
                'paging': {'next': {'after': RESP_AFTER}},
                'results': [RESULT1, RESULT2, ],
            },
        })
        m_requests.get.return_value = m_response

        m_redis.get.return_value = None
        hs.run('test-obj', 5)
        request_params = copy.copy(REQUEST_PARAMS)
        request_params.update({'hapikey': APIKEY})
        m_requests.get.assert_called_once_with(
            'https://api.hubapi.com/crm/v3/objects/test-obj',
            params=request_params
        )
        m_response.json.assert_called_once()
        assert m_publish_object.call_count == 2
        assert m_publish_future.result.call_count == 2
        m_set_last_update.assert_not_called()
        m_task.delay.assert_called_once_with(
            customer_name='test',
            obj_name='test-obj',
            limit=5,
            after=RESP_AFTER
        )

    def test_run_first_import_last_page(self, mock_customerconfig, mock_publisher_client, monkeypatch):
        APIKEY = 'r42hirr4hu3iort3o'
        RESULT1 = {'properties': {'last_update': '2018-03-29T17:55:00Z'}}
        REQUEST_PARAMS = {'fld1': 'val1'}

        mock_customerconfig.get_customer_config.return_value = {
            CONFIG_CUSTOMER_INTEGRATIONS_KEY: {
                Integration.HUBSPOT.value: {
                    'apikey': APIKEY,
                },
            },
        }

        m_redis = Mock(**{'get.return_value': b'bar'})
        m_task = Mock()
        m_requests = Mock()
        m_publish_future = Mock()
        m_publish_object = Mock(return_value=m_publish_future)
        m_set_last_update = Mock()
        hs = HubSpotFetchUpdates('test', m_redis, m_task)

        monkeypatch.setattr(integration, 'requests', m_requests)
        monkeypatch.setattr(hs, 'get_last_update', Mock(return_value=None))
        monkeypatch.setattr(hs, 'set_last_update', m_set_last_update)
        monkeypatch.setattr(hs, 'publish_object', m_publish_object)
        monkeypatch.setattr(hs, 'get_last_update_field', Mock(return_value='last_update'))
        monkeypatch.setattr(hs, 'get_api_request_params', Mock(return_value=REQUEST_PARAMS))

        m_response = Mock(**{
            'status_code': 200,
            'json.return_value': {
                'results': [RESULT1, ],
            },
        })
        m_requests.get.return_value = m_response

        m_redis.get.return_value = None
        hs.run('test-obj', 5)
        request_params = copy.copy(REQUEST_PARAMS)
        request_params.update({'hapikey': APIKEY})
        m_requests.get.assert_called_once_with(
            'https://api.hubapi.com/crm/v3/objects/test-obj',
            params=request_params
        )
        m_response.json.assert_called_once()
        assert m_publish_object.call_count == 1
        assert m_publish_future.result.call_count == 1
        m_set_last_update.assert_called_once_with('test-obj', datetime(2018, 3, 29, 17, 55, 0))
        m_task.delay.assert_not_called()

    def test_run_from_last_time_of_update(self, mock_customerconfig, mock_publisher_client, monkeypatch):
        APIKEY = 'r42hirr4hu3iort3o'
        RESULT1 = {'properties': {'last_update': '2018-03-29T17:55:00Z'}}
        REQUEST_PARAMS = {'fld1': 'val1'}

        mock_customerconfig.get_customer_config.return_value = {
            CONFIG_CUSTOMER_INTEGRATIONS_KEY: {
                Integration.HUBSPOT.value: {
                    'apikey': APIKEY,
                },
            },
        }

        m_redis = Mock(**{'get.return_value': b'bar'})
        m_task = Mock()
        m_requests = Mock()
        m_publish_future = Mock()
        m_publish_object = Mock(return_value=m_publish_future)
        m_set_last_update = Mock()
        hs = HubSpotFetchUpdates('test', m_redis, m_task)

        monkeypatch.setattr(integration, 'requests', m_requests)
        monkeypatch.setattr(hs, 'get_last_update', Mock(return_value=datetime(2018, 3, 29, 17, 54, 0)))
        monkeypatch.setattr(hs, 'set_last_update', m_set_last_update)
        monkeypatch.setattr(hs, 'publish_object', m_publish_object)
        monkeypatch.setattr(hs, 'get_last_update_field', Mock(return_value='last_update'))
        monkeypatch.setattr(hs, 'get_api_request_params', Mock(return_value=REQUEST_PARAMS))

        m_response = Mock(**{
            'status_code': 200,
            'json.return_value': {
                'results': [RESULT1, ],
            },
        })
        m_requests.post.return_value = m_response

        m_redis.get.return_value = None
        hs.run('test-obj', 5)

        m_requests.post.assert_called_once_with(
            'https://api.hubapi.com/crm/v3/objects/test-obj/search',
            params={'hapikey': APIKEY},
            json=REQUEST_PARAMS
        )
        m_response.json.assert_called_once()
        assert m_publish_object.call_count == 1
        assert m_publish_future.result.call_count == 1
        m_set_last_update.assert_called_once_with('test-obj', datetime(2018, 3, 29, 17, 55, 0))
        m_task.delay.assert_not_called()

    def test_run_not_authorized(self, mock_customerconfig, mock_publisher_client, monkeypatch, caplog):
        APIKEY = 'r42hirr4hu3iort3o'
        REQUEST_PARAMS = {'fld1': 'val1'}

        mock_customerconfig.get_customer_config.return_value = {
            CONFIG_CUSTOMER_INTEGRATIONS_KEY: {
                Integration.HUBSPOT.value: {
                    'apikey': APIKEY,
                },
            },
        }

        m_redis = Mock(**{'get.return_value': b'bar'})
        m_task = Mock()
        m_requests = Mock()
        m_publish_future = Mock()
        m_publish_object = Mock(return_value=m_publish_future)
        m_set_last_update = Mock()
        hs = HubSpotFetchUpdates('test', m_redis, m_task)

        monkeypatch.setattr(integration, 'requests', m_requests)
        monkeypatch.setattr(hs, 'get_last_update', Mock(return_value=datetime(2018, 3, 29, 17, 54, 0)))
        monkeypatch.setattr(hs, 'set_last_update', m_set_last_update)
        monkeypatch.setattr(hs, 'publish_object', m_publish_object)
        monkeypatch.setattr(hs, 'get_last_update_field', Mock(return_value='last_update'))
        monkeypatch.setattr(hs, 'get_api_request_params', Mock(return_value=REQUEST_PARAMS))

        m_response = Mock(status_code=401)
        m_requests.post.return_value = m_response

        hs.run('test-obj', 5)

        m_requests.post.assert_called_once_with(
            'https://api.hubapi.com/crm/v3/objects/test-obj/search',
            params={'hapikey': APIKEY},
            json=REQUEST_PARAMS
        )
        assert caplog.record_tuples == [
            (
                'devourer',
                logging.ERROR,
                '[HubSpot: test] unable to fetch test-obj after[None]: 401',
            ),
        ]
        m_response.json.assert_not_called()
        m_publish_object.assert_not_called()
        m_publish_future.assert_not_called()
        m_set_last_update.assert_not_called()
        m_task.delay.assert_not_called()