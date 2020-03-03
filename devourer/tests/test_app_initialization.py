from devourer import config
from devourer.utils import module_loading


def test_init_datasource(monkeypatch):
    testdata = {}

    class TestDataSource:

        def __init__(self, app, url_prefix):
            testdata['app'] = app
            testdata['url_prefix'] = url_prefix

        def __call__(self, customer_name):
            testdata['called'] = True

    monkeypatch.setattr(
        config,
        'CUSTOMERS',
        {
            'test-customer': {
                'datasources': ('test-source', ),
            }
        }
    )
    monkeypatch.setattr(module_loading, "import_string", lambda path: TestDataSource)

    from devourer.main import init_customers

    init_customers(object(), "/api/v-test")
    assert testdata["url_prefix"] == "/api/v-test/test-customer/test-source"
    assert testdata["called"]
