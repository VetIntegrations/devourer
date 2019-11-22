from devourer import config
from devourer.utils import module_loading


def test_init_datasource(monkeypatch):
    testdata = {}

    class TestDataSource:

        def __init__(self, app, url_prefix):
            testdata['app'] = app
            testdata['url_prefix'] = url_prefix

        def __call__(self):
            testdata['called'] = True

    monkeypatch.setattr(config, "DATA_SOURCES", (("test-source", "test_source"), ))
    monkeypatch.setattr(module_loading, "import_string", lambda path: TestDataSource)

    from devourer.main import init_datasources

    init_datasources(object(), "/api/v-test")
    assert testdata["url_prefix"] == "/api/v-test/test-source"
    assert testdata["called"]
