import pytest

from devourer.main import get_application


@pytest.fixture
async def aiohttp_app():
    return await get_application()
