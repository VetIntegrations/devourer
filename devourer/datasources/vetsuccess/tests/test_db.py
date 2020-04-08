from devourer.datasources.vetsuccess import db, tables


def test_table_to_import():
    conn = db.DB(None, None)

    expected = (
        (
            tables.TableConfig(
                name='aaha_accounts',
                checksum_column='id'
            ), None
        ),
        (
            tables.TableConfig(
                name='clients',
                checksum_column='vetsuccess_id',
                order_by='vetsuccess_id'
            ), None
        ),
        (
            tables.TableConfig(
                name='client_attributes',
                checksum_column='vetsuccess_id'
            ), None
        ),
        (
            tables.CodeTableConfig(
                name='codes',
                checksum_column='vetsuccess_id'
            ), db.CodeAdditionalDataFetcher
        ),
        (
            tables.TableConfig(
                name='dates',
                checksum_column='record_date'
            ), None
        ),
        (
            tables.TableConfig(
                name='emails',
                checksum_column='vetsuccess_id',
                order_by='client_vetsuccess_id'
            ), None
        ),
        (
            tables.TableConfig(
                name='invoices',
                timestamp_column='source_updated_at'
            ), None
        ),
        (
            tables.PatientTableConfig(
                name='patients',
                checksum_column='vetsuccess_id',
                order_by='client_vetsuccess_id'
            ), None
        ),
        (
            tables.PatientCoOwnerTableConfig(
                name='client_patient_relationships',
                checksum_column='patient_vetsuccess_id'
            ), None
        ),
        (
            tables.TableConfig(
                name='payment_transactions',
                timestamp_column='source_updated_at'
            ), None
        ),
        (
            tables.TableConfig(
                name='phones',
                checksum_column='vetsuccess_id'
            ), None
        ),
        (
            tables.TableConfig(
                name='practices',
                checksum_column='id'
            ), None
        ),
        (
            tables.TableConfig(
                name='reminders',
                timestamp_column='source_updated_at'
            ), None
        ),
        (
            tables.TableConfig(
                name='resources',
                checksum_column='vetsuccess_id'
            ), None
        ),
        (
            tables.TableConfig(
                name='normalized_transactions',
                timestamp_column='updated_at'
            ), None
        ),
        (
            tables.TableConfig(
                name='schedules',
                timestamp_column='source_updated_at'
            ), None
        ),
        (
            tables.TableConfig(
                name='sites',
                checksum_column='vetsuccess_id'
            ), None
        ),
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
