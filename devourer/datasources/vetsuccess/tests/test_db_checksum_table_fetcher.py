import pytest
from collections import namedtuple
from datetime import datetime

from devourer.datasources.vetsuccess import db


@pytest.mark.parametrize(
    'stor_data, input_data, expected',
    (
        (
            {1: '76ba9bcaaee3e7f329ad1b02f4a1808354ac9084'},
            (1, ('str', 42, datetime(2019, 11, 20, 11, 0)), ),
            (True, '76ba9bcaaee3e7f329ad1b02f4a1808354ac9084'),
        ),
        (
            {1: '76ba9bcaaee3e7f329ad1b02f4a1808354ac9084'},
            (1, ('STR', 42, datetime(2019, 11, 20, 11, 0)), ),
            (False, '0e8a967ad386c1662068badaccdbeebfcdd7f9d3'),
        ),
        (
            {1: '76ba9bcaaee3e7f329ad1b02f4a1808354ac9084'},
            (2, ('str', 42, datetime(2019, 11, 20, 11, 0)), ),
            (False, '76ba9bcaaee3e7f329ad1b02f4a1808354ac9084'),
        ),
    )
)
async def test_is_changed(stor_data, input_data, expected):
    log = []

    class FakeStorage:

        async def __getitem__(self, key):
            return stor_data.get(key)

        def __setitem__(self, key, value):
            log.append(('new-value', key, value))

    stor = FakeStorage()
    fetcher = db.ChecksumTableFether('test', None, None)

    assert len(log) == 0
    assert await fetcher.is_changed(stor, *input_data) == expected[0]
    if expected[0]:
        assert len(log) == 0
    else:
        assert log == [('new-value', input_data[0], expected[1])]


@pytest.mark.parametrize(
    'input_data, expected',
    (
        (1, 1),
        (436728, 436728),
        (datetime.fromtimestamp(1574344680), 1574344680),
    )
)
def test_checksum_column_normalization(input_data, expected):
    assert db.ChecksumTableFether.checksum_column_normalization(input_data) == expected


@pytest.mark.parametrize(
    'tableconfig, input_data, is_changed_vals, expected_result, expected_log',
    (
        (
            db.TableConfig('test', None, 'id'),
            ((1, 'N1', 53), (2, 'N2', 103)),
            (True, True),
            [],
            ['acquire', 'cursor', ('execute', 'SELECT * FROM external.test ORDER BY id')],
        ),
        (
            db.TableConfig('testing', None, 'date'),
            ((1, 'N1', 53), (2, 'N2', 103)),
            (True, False),
            [{'date': 2, 'name': 'N2', 'amount': 103}, ],
            ['acquire', 'cursor', ('execute', 'SELECT * FROM external.testing ORDER BY date')],
        ),
    )
)
async def test_fetch(tableconfig, input_data, is_changed_vals, expected_result, expected_log, monkeypatch):
    log = []

    input_data = iter(input_data)
    is_changed_vals = iter(is_changed_vals)

    async def fake_is_changed(stor, pk, data):
        return next(is_changed_vals)

    fetcher = db.ChecksumTableFether(
        tableconfig,
        FakeDB(tableconfig.checksum_column, input_data, log),
        None
    )
    monkeypatch.setattr(fetcher, 'is_changed', fake_is_changed)

    assert len(log) == 0
    data = []
    async for record in fetcher.fetch():
        data.append(record)

    assert data == expected_result
    assert log == expected_log


Column = namedtuple('Column', 'name')


class FakeDB:

    def __init__(self, checksum_column, input_data, log):
        self.log = log
        self.input_data = input_data
        self.description = (Column(checksum_column), Column('name'), Column('amount'), )

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
