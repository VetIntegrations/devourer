from unittest.mock import Mock, call

from .. import tasks
from ..tasks import hubspot_integration


def test_hubspot_integration(monkeypatch):
    m_hubspot_fetch_updates = Mock()
    m_customerconfig = Mock()
    m_customerconfig.get_customers_with_integration.return_value = [
        (
            'test-customer0',
            {
                'integrations': {
                    'hubspot': {
                        'objects': {
                            'companies': {'priority': 100},
                        },
                    },
                },
            },
        ),
        (
            'test-customer1',
            {
                'integrations': {
                    'hubspot': {
                        'objects': {
                            'companies': {},
                            'deals': {'priority': 10},
                            'whatever': {'priority': 5},
                        },
                    },
                },
            },
        ),
    ]

    monkeypatch.setattr(tasks, 'CustomerConfig', Mock(return_value=m_customerconfig))
    monkeypatch.setattr(tasks, 'hubspot_fetch_updates', m_hubspot_fetch_updates)

    hubspot_integration()
    m_hubspot_fetch_updates.delay.assert_has_calls(
        (
            call(customer_name='test-customer0', obj_name='companies', limit=100),
            call(customer_name='test-customer1', obj_name='companies', limit=100),
            call(customer_name='test-customer1', obj_name='whatever', limit=100),
            call(customer_name='test-customer1', obj_name='deals', limit=100),
        ),
        any_order=False
    )
