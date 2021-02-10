import os
import yaml
from envparse import env
try:
    from yaml import CLoader as YAMLLoader
except ImportError:
    from yaml import Loader as YAMLLoader


env.read_envfile('environment')

DEBUG = env.bool('DEBUG', default=False)

REDIS_HOST = env.str('REDIS_HOST', default='127.0.0.1')
REDIS_PORT = env.int('REDIS_PORT', default=6379)
REDIS_DB = env.int('REDIS_DB', default=1)

# Data Publishing
GCP_PROJECT_ID = env.str('GCP_PROJECT_ID')
GCP_PUBSUB_PUBLIC_TOPIC = env.str('GCP_PUBSUB_PUBLIC_TOPIC')

# Customers config
CUSTOMERS = {
    'rarebreed': {
        'name': 'Rarebreed',
        'datasources': ('vetsuccess', 'bitwerx', ),
    },
}

BITWERX_TIMEOUT = 5 * 60

SENTRY_DSN = env.str('SENTRY_DSN')

CELERY = {
    'broker_url': env.str('CELERY_BROKER_URL'),
    'task_always_eager': env.bool('CELERY_TASK_ALWAYS_EAGER', default=False),
    'task_serializer': 'pickle',
    'result_serializer': 'pickle',
    'accept_content': ('application/x-python-serialize', ),
    'result_backend': env.str('CELERY_RESULT_BACKEND'),
    'timezone': 'UTC',
    'beat_schedule': {

    }
}

# Logging
LOGGING_CONFIG = env.str('LOGGING_CONFIG', default='logging.config.dictConfig')
log_config_path = os.path.join(
    os.path.dirname(__file__),
    'conf/logging',
    env.str('LOG_CONFIG_FILENAME')
)
with open(log_config_path, 'r') as fl:
    LOGGING = yaml.load(fl, Loader=YAMLLoader)
