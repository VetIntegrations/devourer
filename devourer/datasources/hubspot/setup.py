import logging
from devourer.core.datasource import setup


logger = logging.getLogger('devourer.datasource.hubspot')


class DataSourceSetup(setup.DataSourceSetupAbstract):

    def __call__(self, customer_name: str):
        logger.info(f'{customer_name}: HubSpot data source initialization')
