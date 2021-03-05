import logging
import uuid
import typing

from celery import shared_task

from devourer.utils.constants import Integration, CONFIG_CUSTOMER_INTEGRATIONS_KEY
from devourer.utils.customer_config import CustomerConfig
from devourer.utils.waitgroup import WaitGroup, waitgroup_mark, waitgroup_flow
from .integration import HubSpotFetchUpdates


logger = logging.getLogger('devourer')


DATETIME_FORMAT = (
    '%Y-%m-%dT%H:%M:%S.%fZ',
    '%Y-%m-%dT%H:%M:%SZ',
)


@shared_task
def hubspot_integration():
    for customer_name, config in CustomerConfig().get_customers_with_integration(Integration.HUBSPOT):
        hubspot_config = config[CONFIG_CUSTOMER_INTEGRATIONS_KEY][Integration.HUBSPOT.value]
        object_names = sorted(
            hubspot_config['objects'],
            key=lambda name: hubspot_config['objects'][name].get('priority', 0)
        )

        waitgroup_id = uuid.uuid4().hex
        for i, obj_name in enumerate(object_names):
            blocking_waitgroup_key = None
            current_waitgroup_key = f'waitgroup_{customer_name}_{obj_name}_{waitgroup_id}'
            if i != 0:
                blocking_waitgroup_key = f'waitgroup_{customer_name}_{object_names[i - 1]}_{waitgroup_id}'

            kwargs = {
                'customer_name': customer_name,
                'obj_name': obj_name,
                'limit': 100,
                'waitgroup_keys': (blocking_waitgroup_key, current_waitgroup_key)
            }
            waitgroup = WaitGroup(current_waitgroup_key, hubspot_integration.redis)
            waitgroup.add(1)
            hubspot_fetch_updates.delay(**kwargs)


@waitgroup_mark
@shared_task(bind=True)
@waitgroup_flow
def hubspot_fetch_updates(
    self,
    *,
    waitgroup_keys: typing.Tuple[typing.Union[str, None], str],
    customer_name: str,
    obj_name: str,
    limit: int,
    after: str = None,
    is_initial_import=None
):
    _, current_waitgroup_key = waitgroup_keys
    current_waitgroup = WaitGroup(current_waitgroup_key, hubspot_fetch_updates.redis)

    fetcher = HubSpotFetchUpdates(customer_name, hubspot_fetch_updates.redis, hubspot_fetch_updates)
    last_page, data = fetcher.fetch(obj_name, limit, after)

    if not last_page:
        current_waitgroup.add(1)
        hubspot_fetch_updates.delay(
            **{
                'customer_name': customer_name,
                'obj_name': obj_name,
                'limit': limit,
                'waitgroup_keys': waitgroup_keys,
                'after': data['paging']['next']['after'],
                'is_initial_import': is_initial_import,
            },
        )
    fetcher.push(obj_name, last_page, data)
    current_waitgroup.done()
