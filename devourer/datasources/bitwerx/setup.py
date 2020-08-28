import logging
from devourer.core.datasource import setup
from . import api


logger = logging.getLogger('devourer.datasource.bitwerx')


class DataSourceSetup(setup.DataSourceSetupAbstract):

    def __call__(self, customer_name: str):
        logger.info(f'{customer_name}: Bitwerx data source initialization')

        self.app.add_routes([
            self.to_route_view('', api.import_run, customer_name),
        ])
