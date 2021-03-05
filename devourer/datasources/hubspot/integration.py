import copy
import logging
import json
import time
import requests
import celery
import typing
from datetime import datetime
from redis import Redis
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher import futures

from devourer import config
from devourer.utils.constants import Integration, CONFIG_CUSTOMER_INTEGRATIONS_KEY
from devourer.utils.customer_config import CustomerConfig
from devourer.utils import json_helpers, cache
from .exception import (
    HubSpotDatetimeFormatParseException, HubSpotPipelineGettingException, HubSpotAssociationsGettingException
)


logger = logging.getLogger('devourer')


DATETIME_FORMAT = (
    '%Y-%m-%dT%H:%M:%S.%fZ',
    '%Y-%m-%dT%H:%M:%SZ',
)


class HubSpotFetchException(Exception):
    ...


class HubSpotFetchUpdates:

    def __init__(self, customer_name: str, redis: Redis, task: celery.Task):
        self._is_initial_import = None
        self.customer_name = customer_name
        self.redis = redis
        self.customer_config = CustomerConfig().get_customer_config(customer_name)
        self.hubspot_config = self.customer_config[CONFIG_CUSTOMER_INTEGRATIONS_KEY][Integration.HUBSPOT.value]
        self.task = task

        self.publisher = pubsub_v1.PublisherClient()
        self.topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id=config.GCP_PROJECT_ID,
            topic=config.GCP_PUBSUB_PUBLIC_TOPIC,
        )

    def force_set_is_initial_import(self, value: bool):
        self._is_initial_import = value

    def get_last_update_field(self, obj_name: str) -> str:
        return self.hubspot_config['objects'][obj_name]['last_update_field']

    def get_api_request_params(
        self,
        obj_name: str,
        limit: int,
        after: str = None,
        last_update: datetime = None
    ) -> dict:
        params = {
            'limit': limit,
            'properties': self.hubspot_config['objects'][obj_name]['properties'],
            'sorts': [self.get_last_update_field(obj_name)],
        }
        if after:
            params['after'] = after

        if last_update:
            timestamp = time.mktime(last_update.timetuple())
            params['filterGroups'] = [
                {
                    'filters': [
                        {
                            'value': int(timestamp) * 1000,
                            'propertyName': self.get_last_update_field(obj_name),
                            'operator': 'GT',
                        },
                    ],
                }
            ]

        return params

    def get_api_request_auth_params(self):
        return {'hapikey': self.hubspot_config['apikey']}

    def publish_object(self, obj_name: str, obj: dict) -> futures.Future:
        data = {
            'meta': {
                'customer': self.customer_name,
                'data_source': 'hubspot',
                'table_name': obj_name,
                'is_initial_import': self._is_initial_import,
            },
            'data': obj,
        }

        return self.publisher.publish(
            self.topic_name,
            json.dumps(data, cls=json_helpers.JSONEncoder).encode('utf-8')
        )

    def get_last_update(self, obj_name: str) -> bytes:
        timestamp = self.redis.get('last-update__{}_{}'.format(self.customer_name, obj_name))
        if timestamp:
            return datetime.fromtimestamp(int(timestamp))

    def set_last_update(self, obj_name: str, date: datetime):
        timestamp = time.mktime(date.timetuple())
        self.redis.set(
            'last-update__{}_{}'.format(self.customer_name, obj_name),
            int(timestamp)
        )

    @cache.cached_method(60 * 60)
    def get_dealstage_associations(self) -> dict:
        resp = requests.get('https://api.hubapi.com/crm/v3/pipelines/deals', params=self.get_api_request_auth_params())
        if resp.status_code != 200:
            raise HubSpotPipelineGettingException()

        associations = {}
        for pipeline in resp.json()['results']:
            for stage in pipeline.get('stages', []):
                associations[stage['id']] = stage['label']

        return associations

    def _get_obj_association_for_deals(self, ids: typing.List[typing.Union[int, str]]) -> dict:
        association = {}
        resp = requests.post(
            'https://api.hubapi.com/crm/v3/associations/deals/companies/batch/read',
            params=self.get_api_request_auth_params(),
            json={
                'inputs': [{'id': id} for id in ids]
            }
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            raise HubSpotAssociationsGettingException(resp.content)

        for item in resp.json()['results']:
            association[str(item['from']['id'])] = {
                'company_id': item['to'][0]['id'],
            }

        return association

    def _transform_deals(self, data: dict, objs_association: dict) -> dict:
        associations = self.get_dealstage_associations()
        transformed = copy.deepcopy(data)
        if data.get('properties', {}).get('dealstage') in associations:
            transformed['properties']['dealstage'] = associations[data['properties']['dealstage']]

        if str(data['id']) in objs_association:
            transformed['properties'].update(objs_association[str(data['id'])])

        return transformed

    def _process_objects(self, obj_name: str, results: typing.List[dict], last_update: typing.Union[None, datetime]):
        objs_association = {}
        if hasattr(self, f'_get_obj_association_for_{obj_name}'):
            objs_association = getattr(self, f'_get_obj_association_for_{obj_name}')(
                [item['id'] for item in results]
            )

        futures = []
        new_last_update = last_update
        for item in results:
            if hasattr(self, f'_transform_{obj_name}'):
                item = getattr(self, f'_transform_{obj_name}')(item, objs_association)

            futures.append(
                self.publish_object(obj_name, item)
            )

            timestamp = hubspot_datetime_parse(item['properties'][self.get_last_update_field(obj_name)])
            if new_last_update:
                new_last_update = max(new_last_update, timestamp)
            else:
                new_last_update = timestamp

        return futures, new_last_update

    def fetch(self, obj_name: str, limit: int, after: str = None):
        last_update = self.get_last_update(obj_name)
        if self._is_initial_import is None:
            self._is_initial_import = not bool(last_update)

        params = self.get_api_request_params(obj_name, limit, after, last_update)
        auth_params = self.get_api_request_auth_params()
        if last_update:
            resp = requests.post(
                f'https://api.hubapi.com/crm/v3/objects/{obj_name}/search',
                params=auth_params,
                json=params
            )
        else:
            params.update(auth_params)
            resp = requests.get(f'https://api.hubapi.com/crm/v3/objects/{obj_name}', params=params)

        if resp.status_code != 200:
            raise HubSpotFetchException(
                '[HubSpot: {}] unable to fetch {} after[{}]: {}'.format(
                    self.customer_name, obj_name, after, resp.status_code
                )
            )

        last_page = False
        data = resp.json()
        if not data.get('paging', {}).get('next', {}).get('after'):
            last_page = True

        return last_page, data

    def push(self, obj_name: str, last_page: bool, data:  dict):
        last_update = self.get_last_update(obj_name)
        futures, new_last_update = self._process_objects(obj_name, data['results'], last_update)

        if last_page:
            self.set_last_update(obj_name, new_last_update)

        # wait for sending all messages
        for future in futures:
            future.result()


def hubspot_datetime_parse(raw: str) -> datetime:
    ex = None
    for format in DATETIME_FORMAT:
        try:
            dt = datetime.strptime(raw, format)
        except ValueError as exc:
            ex = exc
        else:
            return dt

    raise HubSpotDatetimeFormatParseException() from ex
