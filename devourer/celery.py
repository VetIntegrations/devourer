import os
import logging
import warnings
import sentry_sdk
from celery import Celery
from sentry_sdk.integrations.celery import CeleryIntegration

from devourer import config
from devourer.utils.secret_manager import SecretManager
from devourer.utils.customer_config import CustomerConfig, SecretManagerStorageBackend


logger = logging.getLogger('devourer')


class DiscoverTasks:

    @classmethod
    def discover(cls):
        logger.info('Discover tasks')
        datasource_folder = os.path.join(os.path.dirname(__file__), 'datasources')
        for source_name in os.listdir(datasource_folder):
            tasks_prefix = os.path.join(datasource_folder, source_name, 'tasks')
            if os.path.exists(tasks_prefix + '.py'):
                cls._import(tasks_prefix + '.py')
            elif os.path.isdir(tasks_prefix):
                cls._import_all_in_folder(tasks_prefix)

    @classmethod
    def _import_all_in_folder(cls, path: str):
        for fl_name in os.listdir(path):
            if fl_name.endswith('.py') and fl_name != '__init__.py':
                cls._import(os.path.join(path, fl_name))

    @staticmethod
    def _import(path: str):
        module = path.replace(os.path.dirname(__file__), 'devourer').replace('/', '.')
        if module[-3:] == '.py':
            logger.debug('found task module: {}'.format(module[:-3]))
            __import__(module[:-3])


app = Celery('devourer', task_cls='devourer.utils.celery:DevourerBaseTask')
app.config_from_object(config.CELERY)
if getattr(config, 'SENTRY_DSN'):
    sentry_sdk.init(
        dsn=config.SENTRY_DSN,
        integrations=[CeleryIntegration()]
    )
else:
    warnings.warn('Sentry disabled')

# TODO: move to proper place to run for celery and api
customer_config = CustomerConfig()
customer_config.set_storage_backend(
    SecretManagerStorageBackend(
        SecretManager(config.GCP_PROJECT_ID)
    )
)

DiscoverTasks.discover()
