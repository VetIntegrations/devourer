import logging
from celery import shared_task

from devourer.utils.constants import Integration, CONFIG_CUSTOMER_INTEGRATIONS_KEY
from devourer.utils.customer_config import CustomerConfig
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

        for i, obj_name in enumerate(object_names):
            kwargs = {
                'customer_name': customer_name,
                'obj_name': obj_name,
                'limit': 100,
            }
            hubspot_fetch_updates.delay(**kwargs)


@shared_task
def hubspot_fetch_updates(customer_name: str, obj_name: str, limit: int, after: str = None):
    fetcher = HubSpotFetchUpdates(customer_name, hubspot_fetch_updates.redis, hubspot_fetch_updates)
    fetcher.run(obj_name, limit, after)
