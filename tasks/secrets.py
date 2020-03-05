import json
from invoke import task, exceptions

from devourer.utils import secret_manager


@task(
    help={
        'project': 'GCP project id',
        'key': 'key of the setting, usually name of corporation',
        'value': 'JSON serialized value',
    }
)
def secret_set(c, project, key, value):
    """
    set new or update existing config in GCP secret manager
    """
    try:
        json.loads(value)
    except json.decoder.JSONDecodeError as ex:
        raise exceptions.ParseError('value is not valid JSON object') from ex

    sm = secret_manager.SecretManager(project)
    sm.set_secret(key, value)
