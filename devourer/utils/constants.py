import enum


CONFIG_CUSTOMERS_KEY = 'customers'
CONFIG_CUSTOMER_INTEGRATIONS_KEY = 'integrations'


class Integration(enum.Enum):
    HUBSPOT = 'hubspot'
