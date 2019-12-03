import pytest
from datetime import datetime
from collections import namedtuple

from devourer.datasources.vetsuccess import db, tables


TIMESTAMP = 1574346720


@pytest.mark.parametrize(
    'tableconfig, input_data, expected_result, expected_log',
    (
        (
            tables.TableConfig('test', 'update_at', None),
            ((1, 'N1', 53), (2, 'N2', 103)),
            [{'id': 1, 'name': 'N1', 'amount': 53}, {'id': 2, 'name': 'N2', 'amount': 103}],
            [
                'acquire',
                'cursor',
                ('get', 'devourer.datasource.versuccess.timestamp-test'),
                (
                    'execute',
                    (
                        f"SELECT * FROM external.test "
                        f"WHERE update_at >= '{datetime.fromtimestamp(TIMESTAMP)}'::timestamp ORDER BY id "
                    ),
                ),
                ('set', 'devourer.datasource.versuccess.timestamp-test', TIMESTAMP),
            ],
        ),
        (
            tables.TableConfig('testing', 'refreshed_at', None),
            ((2, 'N2', 103), ),
            [{'id': 2, 'name': 'N2', 'amount': 103}, ],
            [
                'acquire',
                'cursor',
                ('get', 'devourer.datasource.versuccess.timestamp-testing'),
                (
                    'execute',
                    (
                        "SELECT * FROM external.testing "
                        f"WHERE refreshed_at >= '{datetime.fromtimestamp(TIMESTAMP)}'::timestamp "
                        "ORDER BY id "
                    )
                ),
                ('set', 'devourer.datasource.versuccess.timestamp-testing', TIMESTAMP),
            ],
        ),
    )
)
async def test_fetch(tableconfig, input_data, expected_result, expected_log, monkeypatch):
    log = []

    input_data = iter(input_data)

    monkeypatch.setattr(db.time, 'time', lambda: TIMESTAMP)
    fetcher = db.TimestampedTableFetcher(
        tableconfig,
        FakeDB(tableconfig.checksum_column, input_data, log),
        FakeRedis(log, 1574346720)
    )

    assert len(log) == 0
    data = []
    async for record in fetcher.fetch():
        data.append(record)

    assert data == expected_result
    assert log == expected_log


class FakeRedis:

    def __init__(self, log, timestamp):
        self.log = log
        self.timestamp = timestamp

    async def get(self, key):
        self.log.append(('get', key))
        return self.timestamp

    async def set(self, key, value):
        self.log.append(('set', key, value))


Column = namedtuple('Column', 'name')


class FakeDB:

    def __init__(self, checksum_column, input_data, log):
        self.log = log
        self.input_data = input_data
        self.description = (Column('id'), Column('name'), Column('amount'), )

    def acquire(self):
        self.log.append('acquire')
        return self

    def cursor(self):
        self.log.append('cursor')
        return self

    async def execute(self, sql):
        self.log.append(('execute', sql))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        ...

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.input_data)
        except StopIteration:
            raise StopAsyncIteration
