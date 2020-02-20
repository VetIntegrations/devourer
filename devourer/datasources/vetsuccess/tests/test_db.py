from devourer.datasources.vetsuccess import db, tables


def test_table_to_import():
    conn = db.DB(None, None)

    expected = (
        (tables.TableConfig('aaha_accounts', None, 'id'), None),
        (tables.TableConfig('clients', None, 'id', 'vetsuccess_id'), None),
        (tables.TableConfig('client_attributes', None, 'id'), None),
        (tables.CodeTableConfig('codes', None, 'id'), db.CodeAdditionalDataFetcher),
        (tables.TableConfig('dates', None, 'record_date'), None),
        (tables.TableConfig('emails', None, 'id', 'client_vetsuccess_id'), None),
        (tables.TableConfig('invoices', 'source_updated_at', None), None),
        (tables.PatientTableConfig('patients', None, 'vetsuccess_id', 'client_vetsuccess_id'), None),
        (tables.TableConfig('payment_transactions', 'source_updated_at', None), None),
        (tables.TableConfig('phones', None, 'id'), None),
        (tables.TableConfig('practices', None, 'id'), None),
        (tables.TableConfig('reminders', 'source_updated_at', None), None),
        (tables.TableConfig('resources', None, 'id'), None),
        (tables.TableConfig('revenue_transactions', 'source_updated_at', None), None),
        (tables.TableConfig('schedules', 'source_updated_at', None), None),
        (tables.TableConfig('sites', None, 'id'), None),
    )

    for (table, addtional_fetcher), (expect_table, expect_additonal_fetcher) in zip(conn.get_tables(), expected):
        assert table.__class__ == expect_table.__class__
        assert table.name == expect_table.name
        assert table.timestamp_column == expect_table.timestamp_column
        assert table.checksum_column == expect_table.checksum_column
        assert table.order_by == expect_table.order_by


async def test_get_updates(monkeypatch):
    log = []

    class FakeAdditionalFetcher:

        @staticmethod
        async def fetch(data, *args):
            log.append('additional_fetcher')

            return data['id'] + 5

    def get_tables():
        return (
            (tables.TableConfig('test-checksum', None, 'id'), FakeAdditionalFetcher),
            (tables.TableConfig('test-timestamped', 'updated_at', None), None),
        )

    monkeypatch.setattr(
        db,
        'TimestampedTableFetcher',
        FakeFetcher.build('timestampled-fetcher', [{'id': 1}, {'id': 2}])
    )
    monkeypatch.setattr(
        db,
        'ChecksumTableFether',
        FakeFetcher.build('checksum-fetcher', [{'id': 10}, {'id': 20}])
    )

    _db = db.DB(None, None)
    monkeypatch.setattr(_db, 'get_tables', get_tables)

    result = []
    async for ret in _db.get_updates():
        result.append(ret)

    assert result == [
        ('test-checksum', {'id': 10, '_additionals': 15}),
        ('test-checksum', {'id': 20, '_additionals': 25}),
        ('test-timestamped', {'id': 1}),
        ('test-timestamped', {'id': 2}),
    ]
    assert log == ['additional_fetcher', 'additional_fetcher']


class FakeFetcher:

    @classmethod
    def build(cls, name, data):
        def builder(*args):
            return cls(name, data, *args)

        return builder

    def __init__(self, name, data, *args):
        self.name = name
        self.data = data

    async def fetch(self):
        for record in self.data:
            yield record
