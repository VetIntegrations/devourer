import aiopg
import aioredis
import logging
import time
import typing
from collections import namedtuple
from datetime import datetime, date
from hashlib import sha1

from devourer import config


logger = logging.getLogger('devourer.datasource.vetsuccess')


TableConfig = namedtuple('TableConfig', ('name', 'timestamp_column', 'checksum_column'))


class DB:

    def __init__(self, db: aiopg.Pool, redis: aioredis.ConnectionsPool):
        self._db = db
        self._redis = redis

    async def get_updates(self) -> typing.AsyncGenerator[typing.Tuple[str, dict], None]:
        start = time.time()
        total_new_records = 0
        new_records = 0
        for table in self.get_tables():
            table_start = time.time()
            fetcher_class = TimestampedTableFetcher
            if table.timestamp_column is None:
                fetcher_class = ChecksumTableFether

            fetcher = fetcher_class(table, self._db, self._redis)

            new_records = 0
            async for record in fetcher.fetch():
                new_records += 1
                yield (table.name, record)

            total_new_records += new_records
            working_time = time.time() - table_start
            logger.info(f'import {table.name} for {working_time} sec, {new_records} new records')

        total = time.time() - start
        logger.info(f'import VetSuccess for {total} sec, {total_new_records} new records')

    def get_tables(self) -> typing.Iterable[TableConfig]:
        return (
            TableConfig('aaha_accounts', None, 'id'),
            TableConfig('clients', None, 'id'),
            TableConfig('client_attributes', None, 'id'),
            TableConfig('code_tag_mappings', None, 'id'),
            TableConfig('code_tags', None, 'id'),
            TableConfig('codes', None, 'id'),
            TableConfig('dates', None, 'record_date'),
            TableConfig('emails', None, 'id'),
            TableConfig('invoices', 'source_updated_at', None),
            # TableConfig('monitoring_recent_dates', None),
            TableConfig('patients', None, 'id'),
            TableConfig('payment_transactions', 'source_updated_at', None),
            TableConfig('phones', None, 'id'),
            TableConfig('practices', None, 'id'),
            TableConfig('reminders', 'source_updated_at', None),
            TableConfig('resources', None, 'id'),
            TableConfig('revenue_categories_hierarchy', None, 'id'),
            TableConfig('revenue_transactions', 'source_updated_at', None),
            TableConfig('schedules', 'source_updated_at', None),
            TableConfig('sites', None, 'id'),
        )


class ChecksumStorage:

    def __init__(self, table_name: str, redis: aioredis.ConnectionsPool, block_size: int = 1000):
        self.table_name = table_name
        self.redis = redis
        self.block_size = block_size
        self.checksums = {}
        self.updated = {}
        self.block_range = (-1, -1)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.sync_current_block()

    async def __getitem__(self, pk: int) -> str:
        if pk < self.block_range[0] or pk > self.block_range[1]:
            await self.sync_current_block()
            self.checksums = await self.get_block(pk)

        return self.checksums.get(str(pk))

    def __setitem__(self, pk: int, checksum: str):
        self.updated[pk] = checksum

    async def get_block(self, pk: int):
        data = await self.redis.hgetall(self.get_storage_key(pk), encoding='utf-8')
        self.block_range = (
            pk // self.block_size * self.block_size,
            pk // self.block_size * self.block_size + self.block_size
        )

        return data

    async def sync_current_block(self):
        if self.updated:
            await self.redis.hmset_dict(
                self.get_storage_key(next(iter(self.updated.keys()))),
                self.updated
            )
            self.updated = {}

    def get_storage_key(self, pk: int) -> str:
        return 'devourer.datasource.versuccess.checksums-{}-{}'.format(
            self.table_name,
            pk // self.block_size
        )


class TimestampStorage:

    def __init__(self, table_name: str, redis: aioredis.ConnectionsPool):
        self.table_name = table_name
        self.redis = redis

    async def __aenter__(self):
        self.new_timestamp = int(time.time())
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.redis.set(self.get_storage_key(), self.new_timestamp)

    async def get_latest(self) -> int:
        ts = await self.redis.get(self.get_storage_key())

        if ts is None:
            ts = 0

        return int(ts)

    def get_storage_key(self) -> str:
        return 'devourer.datasource.versuccess.timestamp-{}'.format(
            self.table_name
        )


class TimestampedTableFetcher:

    def __init__(self, table: TableConfig, db: aiopg.Pool, redis: aioredis.ConnectionsPool):
        self.table = table
        self.db = db
        self.redis = redis

    async def fetch(self):
        async with self.db.acquire() as conn:
            async with conn.cursor() as cur:
                async with TimestampStorage(self.table.name, self.redis) as stor:
                    timestamp = datetime.fromtimestamp(await stor.get_latest())
                    await cur.execute(
                        f'SELECT * FROM external.{self.table.name} '
                        f"WHERE {self.table.timestamp_column} >= '{timestamp}'::timestamp "
                    )

                    column_names = [
                        column.name
                        for column in cur.description
                    ]

                    async for rawdata in cur:
                        yield dict(zip(column_names, rawdata))


class ChecksumTableFether:

    def __init__(self, table: TableConfig, db: aiopg.Pool, redis: aioredis.ConnectionsPool):
        self.table = table
        self.db = db
        self.redis = redis
        self._checksums_range = (-1, -1)
        self._checksums = {}

    async def fetch(self):
        async with self.db.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f'SELECT * FROM external.{self.table.name} '
                    f'ORDER BY {self.table.checksum_column}'
                )

                column_names = [
                    column.name
                    for column in cur.description
                ]

                async with ChecksumStorage(self.table.name, self.redis) as stor:
                    async for rawdata in cur:
                        data = dict(zip(column_names, rawdata))
                        is_changed = await self.is_changed(
                            stor,
                            self.checksum_column_normalization(data[self.table.checksum_column]),
                            rawdata
                        )
                        if not is_changed:
                            yield data

    @staticmethod
    def checksum_column_normalization(value):
        if isinstance(value, (datetime, date)):
            return int(time.mktime(value.timetuple()))

        return value

    async def is_changed(self, stor: ChecksumStorage, pk: int, data: typing.Iterable) -> bool:
        checksum = sha1(':'.join(map(str, data)).encode('utf-8')).hexdigest()

        if (await stor[pk]) != checksum:
            stor[pk] = checksum

            return False

        return True


async def connect(redis: aioredis.ConnectionsPool) -> DB:
    pool = await aiopg.create_pool(
        config.VETSUCCESS_REDSHIFT_DSN,
        enable_hstore=False
    )

    return DB(pool, redis)
