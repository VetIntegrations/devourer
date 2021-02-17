import logging
import json
import time
import requests
import celery
from datetime import datetime
from redis import Redis
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher import futures

from devourer import config
from devourer.utils.constants import Integration, CONFIG_CUSTOMER_INTEGRATIONS_KEY
from devourer.utils.customer_config import CustomerConfig
from devourer.utils import json_helpers
from .exception import HubSpotDatetimeFormatParseException


logger = logging.getLogger('devourer')


DATETIME_FORMAT = (
    '%Y-%m-%dT%H:%M:%S.%fZ',
    '%Y-%m-%dT%H:%M:%SZ',
)


class HubSpotFetchUpdates:

    def __init__(self, customer_name: str, redis: Redis, task: celery.Task):
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

    def get_last_update_field(self, obj_name: str) -> str:
        return self.hubspot_config['objects'][obj_name]['last_update_field']

    def get_api_request_params(self, obj_name: str, limit: int, after: str = None, last_update: bytes = None) -> dict:
        params = {
            'limit': limit,
            'properties': self.hubspot_config['objects'][obj_name]['properties'],
            'sorts': [self.get_last_update_field(obj_name)],
        }
        if after:
            params['after'] = after

        if last_update:
            params['filterGroups'] = {
                'filters': [
                    {
                        'value': int(last_update) * 1000,
                        'propertyName': self.get_last_update_field(obj_name),
                        'operator': 'GT',
                    },
                ],
            }

        return params

    def publish_object(self, obj_name: str, obj: dict) -> futures.Future:
        data = {
            'meta': {
                'customer': self.customer_name,
                'data_source': 'hubspot',
                'table_name': obj_name,
            },
            'data': obj,
        }

        return self.publisher.publish(
            self.topic_name,
            json.dumps(data, cls=json_helpers.JSONEncoder).encode('utf-8')
        )

    def get_last_update(self, obj_name: str) -> bytes:
        return self.redis.get('last-update__{}_{}'.format(self.customer_name, obj_name))

    def set_last_update(self, obj_name: str, date: datetime):
        timestamp = time.mktime(date.timetuple())
        self.redis.set(
            'last-update__{}_{}'.format(self.customer_name, obj_name),
            int(timestamp)
        )

    def run(self, obj_name: str, limit: int, after: str = None):
        last_update = self.get_last_update(obj_name)
        params = self.get_api_request_params(obj_name, limit, after, last_update)
        auth_params = {'hapikey': self.hubspot_config['apikey']}
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
            logger.error(
                '[HubSpot: {}] unable to fetch {} after[{}]: {}'.format(
                    self.customer_name, obj_name, after, resp.status_code
                )
            )
            return

        last_page = False
        data = resp.json()
        if data.get('paging', {}).get('next', {}).get('after'):
            self.task.delay(
                customer_name=self.customer_name,
                obj_name=obj_name,
                limit=limit,
                after=data['paging']['next']['after']
            )
        else:
            last_page = True

        futures = []
        new_last_update = last_update
        for item in data['results']:
            futures.append(
                self.publish_object(obj_name, item)
            )

            timestamp = hubspot_datetime_parse(item['properties'][self.get_last_update_field(obj_name)])
            if new_last_update:
                new_last_update = max(new_last_update, timestamp)
            else:
                new_last_update = timestamp

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
