import json
from google.api_core.exceptions import NotFound
from google.cloud import secretmanager_v1beta1 as secretmanager


class SecretManager:

    def __init__(self, project_id: str):
        self._project_id = project_id
        self._cli = secretmanager.SecretManagerServiceClient()
        self._project_path = self._cli.project_path(self._project_id)

    def set_secret(self, name: str, data: dict):
        try:
            secret = self._cli.get_secret(self._cli.secret_path(self._project_id, name))
        except NotFound:
            secret = self._cli.create_secret(
                self._project_path,
                name,
                {'replication': {'automatic': {}}}
            )

        self._cli.add_secret_version(
            secret.name,
            {
                'data': json.dumps(data).encode('utf-8'),
            }
        )

    def get_secret(self, name: str, version: str = 'latest') -> dict:
        version = self._cli.access_secret_version(
            self._cli.secret_version_path(self._project_id, name, version)
        )

        return json.loads(version.payload.data)
