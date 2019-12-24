import logging
import logging.config
import google.cloud.logging
from google.cloud.logging.handlers.handlers import CloudLoggingHandler

from devourer import config
from . import module_loading


DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'devourer.utils.log.RequireDebugFalse',
        },
        'require_debug_true': {
            '()': 'devourer.utils.log.RequireDebugTrue',
        },
    },
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {thread:d} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'filters': ['require_debug_true'],
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'devourer': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    }
}


def configure_logging(logging_config, logging_settings):
    if logging_config:
        # First find the logging configuration function ...
        logging_config_func = module_loading.import_string(logging_config)

        logging.config.dictConfig(DEFAULT_LOGGING)

        # ... then invoke it with the logging settings
        if logging_settings:
            logging_config_func(logging_settings)


class RequireDebugFalse(logging.Filter):
    def filter(self, record):
        return not config.DEBUG


class RequireDebugTrue(logging.Filter):
    def filter(self, record):
        return config.DEBUG


class GCPLoggingHandler(CloudLoggingHandler):

    def __init__(self, *args, **kwargs):
        client = google.cloud.logging.Client()

        super().__init__(client, *args, **kwargs)
