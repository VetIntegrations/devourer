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


DATA_SOURCES = (
    ("vetsuccess", "vetsuccess"),
)


# DataSource: VetSuccess
VETSUCCESS_REDSHIFT_DSN = env.str("VETSUCCESS_REDSHIFT_DSN", default=None)


# Logging
LOGGING_CONFIG = env.str('LOGGING_CONFIG', default='logging.config.dictConfig')
log_config_path = os.path.join(
    os.path.dirname(__file__),
    'conf/logging',
    env.str('LOG_CONFIG_FILENAME')
)
with open(log_config_path, 'r') as fl:
    LOGGING = yaml.load(fl, Loader=YAMLLoader)
