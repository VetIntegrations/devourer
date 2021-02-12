import abc

from devourer import config
from .constants import Integration, CONFIG_CUSTOMERS_KEY, CONFIG_CUSTOMER_INTEGRATIONS_KEY
from .secret_manager import SecretManager
from .singleton import Singleton


class StorageBackendInterface(abc.ABC):

    @abc.abstractmethod
    def get(self, name: str):
        pass


class SecretManagerStorageBackend(StorageBackendInterface):

    def __init__(self, secret_manager: SecretManager):
        self.secret_manager = secret_manager

    def get(self, name: str):
        return self.secret_manager.get_secret(name)


class CustomerConfig(metaclass=Singleton):

    def set_storage_backend(self, backend: StorageBackendInterface):
        self.storage_backend = backend

    def get_customer_config(self, name: str):
        return self.get_customers()[name]

    def get_customers_with_integration(self, integration: Integration):
        return [
            (name, config)
            for name, config in self.get_customers().items()
            if self.has_integration(config, integration.value)
        ]

    def get_customers(self):
        return self.storage_backend.get(config.CONFIG_NAME)[CONFIG_CUSTOMERS_KEY]

    @staticmethod
    def has_integration(customer: dict, integration: Integration):
        return integration in customer.get(CONFIG_CUSTOMER_INTEGRATIONS_KEY, {})
