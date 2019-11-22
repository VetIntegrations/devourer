from devourer.datasources.vetsuccess import db


def test_table_to_import():
    conn = db.DB(None, None)

    assert conn.get_tables() == (
        db.TableConfig('aaha_accounts', None, 'id'),
        db.TableConfig('clients', None, 'id'),
        db.TableConfig('client_attributes', None, 'id'),
        db.TableConfig('code_tag_mappings', None, 'id'),
        db.TableConfig('code_tags', None, 'id'),
        db.TableConfig('codes', None, 'id'),
        db.TableConfig('dates', None, 'record_date'),
        db.TableConfig('emails', None, 'id'),
        db.TableConfig('invoices', 'source_updated_at', None),
        # db.TableConfig('monitoring_recent_dates', None),
        db.TableConfig('patients', None, 'id'),
        db.TableConfig('payment_transactions', 'source_updated_at', None),
        db.TableConfig('phones', None, 'id'),
        db.TableConfig('practices', None, 'id'),
        db.TableConfig('reminders', 'source_updated_at', None),
        db.TableConfig('resources', None, 'id'),
        db.TableConfig('revenue_categories_hierarchy', None, 'id'),
        db.TableConfig('revenue_transactions', 'source_updated_at', None),
        db.TableConfig('schedules', 'source_updated_at', None),
        db.TableConfig('sites', None, 'id'),
    )


async def test_get_updates(monkeypatch):
    def get_tables():
        return (
            db.TableConfig('test-checksum', None, 'id'),
            db.TableConfig('test-timestamped', 'updated_at', None),
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
