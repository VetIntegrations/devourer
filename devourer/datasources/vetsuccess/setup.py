import logging
from devourer.core.datasource import setup
from . import api


logger = logging.getLogger('devourer.datasource.vetsuccess')


class DataSourceSetup(setup.DataSourceSetupAbstract):

    def __call__(self, customer_name: str, options: dict):
        logger.info(f'{customer_name}: VetSuccess data source initialization')

        self.app.add_routes([
            self.to_route_view('', api.import_run, customer_name, options),
        ])
