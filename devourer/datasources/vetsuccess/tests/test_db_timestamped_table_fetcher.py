import pytest
from collections import namedtuple
from datetime import datetime

from devourer.datasources.vetsuccess import db, tables


TIMESTAMP = 1574346720  # 2019-11-21T16:32:00
TIMESTAMP_DT = datetime.fromtimestamp(TIMESTAMP).strftime('%Y-%m-%dT%H:%M:%S')

TIMESTAMP_LINE_1 = "1574346730"
TIMESTAMP_LINE_2 = "1574346732"  # +2 seconds


@pytest.mark.parametrize(
    'tableconfig, input_data, expected_result, expected_log',
    (
        (
            tables.TableConfig('test', 'update_at', None),
            ((1, 'N1', 53, TIMESTAMP_LINE_1), (2, 'N2', 103, TIMESTAMP_LINE_2)),
            [
                {'id': 1, 'name': 'N1', 'amount': 53, 'update_at': TIMESTAMP_LINE_1},
                {'id': 2, 'name': 'N2', 'amount': 103, 'update_at': TIMESTAMP_LINE_2}
            ],
            [
                'acquire',
                'cursor',
                ('get', 'devourer.datasource.versuccess.timestamp-test'),
                (
                    'execute',
                    (
                        f"SELECT * FROM external.test "
                        f"WHERE update_at >= '{TIMESTAMP_DT}'::timestamp "
                        "ORDER BY id  LIMIT 10000 OFFSET 0"
                    ),
                ),
                ('set', 'devourer.datasource.versuccess.timestamp-test', int(TIMESTAMP_LINE_2)),  # set last timestamp
            ],
        ),
        (
            tables.TableConfig('testing', 'update_at', None),
            ((2, 'N2', 103, TIMESTAMP_LINE_1), ),
            [{'id': 2, 'name': 'N2', 'amount': 103, 'update_at': TIMESTAMP_LINE_1}, ],
            [
                'acquire',
                'cursor',
                ('get', 'devourer.datasource.versuccess.timestamp-testing'),
                (
                    'execute',
                    (
                        "SELECT * FROM external.testing "
                        f"WHERE update_at >= '{TIMESTAMP_DT}'::timestamp "
                        "ORDER BY id  LIMIT 10000 OFFSET 0"
                    )
                ),
                ('set', 'devourer.datasource.versuccess.timestamp-testing', int(TIMESTAMP_LINE_1)),
            ],
        ),
    )
)
async def test_fetch(tableconfig, input_data, expected_result, expected_log):
    log = []

    input_data = iter(input_data)

    fetcher = db.TimestampedTableFetcher(
        tableconfig,
        FakeDB(input_data, log),
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

    def __init__(self, input_data, log):
        self.log = log
        self.input_data = input_data
        self.description = (Column('id'), Column('name'), Column('amount'), Column('update_at'))

    def acquire(self):
        self.log.append('acquire')
        return self

    def cursor(self):
        self.log.append('cursor')
        return self

    @property
    def rowcount(self):
        return 0

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
