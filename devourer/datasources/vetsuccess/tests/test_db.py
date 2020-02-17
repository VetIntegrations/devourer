from devourer.datasources.vetsuccess import db, tables


def test_table_to_import():
    conn = db.DB(None, None)

    expected = (
        tables.TableConfig('aaha_accounts', None, 'id'),
        tables.TableConfig('clients', None, 'id', 'vetsuccess_id'),
        tables.TableConfig('client_attributes', None, 'id'),
        tables.TableConfig('code_tag_mappings', None, 'id'),
        tables.TableConfig('code_tags', None, 'id'),
        tables.TableConfig('codes', None, 'id'),
        tables.TableConfig('dates', None, 'record_date'),
        tables.TableConfig('emails', None, 'id', 'client_vetsuccess_id'),
        tables.TableConfig('invoices', 'source_updated_at', None),
        tables.PatientTableConfig('patients', None, 'vetsuccess_id', 'client_vetsuccess_id'),
        tables.TableConfig('payment_transactions', 'source_updated_at', None),
        tables.TableConfig('phones', None, 'id'),
        tables.TableConfig('practices', None, 'id'),
        tables.TableConfig('reminders', 'source_updated_at', None),
        tables.TableConfig('resources', None, 'id'),
        tables.TableConfig('revenue_categories_hierarchy', None, 'id'),
        tables.TableConfig('revenue_transactions', 'source_updated_at', None),
        tables.TableConfig('schedules', 'source_updated_at', None),
        tables.TableConfig('sites', None, 'id'),
    )

    for table, expect in zip(conn.get_tables(), expected):
        assert table.__class__ == expect.__class__
        assert table.name == expect.name
        assert table.timestamp_column == expect.timestamp_column
        assert table.checksum_column == expect.checksum_column
        assert table.order_by == expect.order_by


async def test_get_updates(monkeypatch):
    def get_tables():
        return (
            tables.TableConfig('test-checksum', None, 'id'),
            tables.TableConfig('test-timestamped', 'updated_at', None),
        )

    monkeypatch.setattr(db, 'TimestampedTableFetcher', FakeFetcher.build('timestampled-fetcher', [1, 2]))
    monkeypatch.setattr(db, 'ChecksumTableFether', FakeFetcher.build('checksum-fetcher', [10, 20]))

    _db = db.DB(None, None)
    monkeypatch.setattr(_db, 'get_tables', get_tables)

    result = []
    async for ret in _db.get_updates():
        result.append(ret)

    assert result == [
        ('test-checksum', 10),
        ('test-checksum', 20),
        ('test-timestamped', 1),
        ('test-timestamped', 2),
    ]


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
