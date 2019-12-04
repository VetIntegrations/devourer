import aiopg
import aioredis
import logging
import time
import typing
from datetime import datetime, date
from hashlib import sha1

from . import tables


logger = logging.getLogger('devourer.datasource.vetsuccess')


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

    def get_tables(self) -> typing.Iterable[tables.TableConfig]:
        return (
            tables.TableConfig('aaha_accounts', None, 'id'),
            tables.TableConfig('clients', None, 'id', 'vetsuccess_id'),
            tables.TableConfig('client_attributes', None, 'id'),
            tables.TableConfig('code_tag_mappings', None, 'id'),
            tables.TableConfig('code_tags', None, 'id'),
            tables.TableConfig('codes', None, 'id'),
            tables.TableConfig('dates', None, 'record_date'),
            tables.TableConfig('emails', None, 'id', 'client_vetsuccess_id'),
            tables.TableConfig('invoices', 'source_updated_at', None),
            tables.PatientTableConfig('patients', None, 'id', 'client_vetsuccess_id'),
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

    def __init__(self, table: tables.TableConfig, db: aiopg.Pool, redis: aioredis.ConnectionsPool):
        self.table = table
        self.db = db
        self.redis = redis

    async def fetch(self):
        async with self.db.acquire() as conn:
            async with conn.cursor() as cur:
                async with TimestampStorage(self.table.name, self.redis) as stor:
                    timestamp = datetime.fromtimestamp(await stor.get_latest())
                    await cur.execute(self.table.get_sql() % {'timestamp': timestamp})

                    column_names = [
                        column.name
                        for column in cur.description
                    ]

                    async for rawdata in cur:
                        yield dict(zip(column_names, rawdata))


class ChecksumTableFether:

    def __init__(self, table: tables.TableConfig, db: aiopg.Pool, redis: aioredis.ConnectionsPool):
        self.table = table
        self.db = db
        self.redis = redis
        self._checksums_range = (-1, -1)
        self._checksums = {}

    async def fetch(self):
        async with self.db.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(self.table.get_sql())

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


async def connect(dsn: str, redis: aioredis.ConnectionsPool) -> DB:
    pool = await aiopg.create_pool(dsn, enable_hstore=False)

    return DB(pool, redis)
