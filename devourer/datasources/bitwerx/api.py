from aiohttp import web


async def import_run(request, customer_name: str = None) -> web.Response:
    config = request.app['secretmanager'].get_secret(customer_name)['bitwerx']

    return web.Response(status=200)
