import logging
from devourer.core.datasource import setup
from . import api


logger = logging.getLogger('devourer.datasource.vetsuccess')


class DataSourceSetup(setup.DataSourceSetupAbstract):

    def __call__(self):
        logger.info('VetSuccess data source initialization')

        self.app.add_routes([
            self.to_route_view('import', api.VetSuccessImportRun),
        ])
