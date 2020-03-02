import abc
import functools
import os
import typing
from aiohttp import web


class DataSourceSetupAbstract(abc.ABC):

    def __init__(self, app: web.Application, url_prefix: str):
        self.app = app
        self.url_prefix = url_prefix

    @abc.abstractmethod
    def __call__(self):
        ...

    def to_route_view(
        self,
        path: str,
        view: typing.Type[web.View],
        customer_name: str
    ) -> web.RouteDef:
        return web.view(
            os.path.join(self.url_prefix, path),
            functools.partial(view, customer_name=customer_name)
        )
