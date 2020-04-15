import logging
import time
import typing
import itertools
import aiopg
import aioredis
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
        for (table, additional_data_fetcher) in self.get_tables():
            table_start = time.time()
            fetcher_class = TimestampedTableFetcher
            if table.timestamp_column is None:
                fetcher_class = ChecksumTableFether

            fetcher = fetcher_class(table, self._db, self._redis)

            new_records = 0
            async for record in fetcher.fetch():
                new_records += 1
                if additional_data_fetcher:
                    record['_additionals'] = await additional_data_fetcher.fetch(record, table, self._db)
                yield (table.name, record)
                if new_records % 1000 == 0:
                    logger.info('import progress: %d of %s', new_records, table.name)

            total_new_records += new_records
            working_time = time.time() - table_start
            logger.info(f'import {table.name} for {working_time} sec, {new_records} new records')

        total = time.time() - start
        logger.info(f'import VetSuccess for {total} sec, {total_new_records} new records')

    async def close(self):
        self._db.close()

    def get_tables(self) -> typing.Iterable[typing.Tuple[tables.TableConfig, typing.Any]]:
        return (
            # (
            #     tables.TableConfig(
            #         name='aaha_accounts',
            #         checksum_column='id'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='clients',
            #         checksum_column='vetsuccess_id',
            #         order_by='vetsuccess_id'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='client_attributes',
            #         checksum_column='vetsuccess_id'
            #     ), None
            # ),
            # (
            #     tables.CodeTableConfig(
            #         name='codes',
            #         checksum_column='vetsuccess_id'
            #     ), CodeAdditionalDataFetcher
            # ),
            # (
            #     tables.TableConfig(
            #         name='dates',
            #         checksum_column='record_date'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='emails',
            #         checksum_column='vetsuccess_id',
            #         order_by='client_vetsuccess_id'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='invoices',
            #         timestamp_column='source_updated_at'
            #     ), None
            # ),
            # (
            #     tables.PatientTableConfig(
            #         name='patients',
            #         checksum_column='vetsuccess_id',
            #         order_by='client_vetsuccess_id'
            #     ), None
            # ),
            # (
            #     tables.PatientCoOwnerTableConfig(
            #         name='client_patient_relationships',
            #         checksum_column='patient_vetsuccess_id'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='payment_transactions',
            #         timestamp_column='source_updated_at'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='phones',
            #         checksum_column='vetsuccess_id'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='practices',
            #         checksum_column='id'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='reminders',
            #         timestamp_column='source_updated_at'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='resources',
            #         checksum_column='vetsuccess_id'
            #     ), None
            # ),
            (
                tables.TableConfig(
                    name='normalized_transactions',
                    timestamp_column='updated_at'
                ), None
            ),
            # (
            #     tables.TableConfig(
            #         name='schedules',
            #         timestamp_column='source_updated_at'
            #     ), None
            # ),
            # (
            #     tables.TableConfig(
            #         name='sites',
            #         checksum_column='vetsuccess_id'
            #     ), None
            # ),
        )


class ChecksumStorage:
    THRESHOLD = 1000

    def __init__(self, table_name: str, redis: aioredis.ConnectionsPool):
        self.table_name = table_name
        self.redis = redis
        self.checksums = None
        self.updated = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.sync_current_block()

    async def __getitem__(self, pk: int) -> str:
        if self.checksums is None:
            self.checksums = await self.get_block()

        return self.checksums.get(str(pk))

    def __setitem__(self, pk: int, checksum: str):
        self.updated[pk] = checksum

    async def set(self, pk: int, checksum: str):
        self[pk] = checksum
        if len(self.updated) > self.THRESHOLD:
            await self.sync_current_block()

    async def get_block(self):
        data = await self.redis.hgetall(self.get_storage_key(), encoding='utf-8')

        return data or {}

    async def sync_current_block(self):
        if self.updated:
            await self.redis.hmset_dict(
                self.get_storage_key(),
                self.updated
            )
            self.updated = {}

    def get_storage_key(self) -> str:
        return 'devourer.datasource.versuccess.checksums-{}'.format(
            self.table_name
        )


class TimestampStorage:
    SAVE_THRESHOLD = 1000

    def __init__(self, table_name: str, redis: aioredis.ConnectionsPool):
        self.table_name = table_name
        self.redis = redis
        self.timestamp = None
        self._debounce = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.timestamp:
            await self._save_to_redis()

    async def get_latest(self) -> int:
        ts = await self.redis.get(self.get_storage_key())

        if ts is None:
            last_update = datetime(1, 1, 1)
        else:
            last_update = datetime.fromtimestamp(int(ts))

        return last_update

    async def set_timestamp(self, row_time):
        if isinstance(row_time, datetime):
            row_time = time.mktime(row_time.timetuple())
        self.timestamp = int(row_time)

        self._debounce += 1
        if self._debounce > self.SAVE_THRESHOLD:
            await self._save_to_redis()
            self._debounce = 0

    def get_storage_key(self) -> str:
        return 'devourer.datasource.versuccess.timestamp-{}'.format(
            self.table_name
        )

    async def _save_to_redis(self):
        await self.redis.set(self.get_storage_key(), self.timestamp)


class TimestampedTableFetcher:

    def __init__(self, table: tables.TableConfig, db: aiopg.Pool, redis: aioredis.ConnectionsPool):
        self.table = table
        self.db = db
        self.redis = redis

    async def fetch(self):
        async with self.db.acquire() as conn:
            async with conn.cursor() as cur:
                async with TimestampStorage(self.table.name, self.redis) as stor:
                    timestamp = await stor.get_latest()
                    sql = self.table.get_sql() % {'timestamp': timestamp.isoformat()}
                    offset = 0
                    limit = 500000
                    while True:
                        await cur.execute(f'{sql} LIMIT {limit} OFFSET {offset}', timeout=60 * 15)

                        column_names = [
                            column.name
                            for column in cur.description
                        ]

                        async for rawdata in cur:
                            data = dict(zip(column_names, rawdata))
                            yield data
                            await stor.set_timestamp(data[self.table.timestamp_column])

                        if cur.rowcount == 0:
                            break

                        offset += limit


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
                sql = self.table.get_sql()
                offset = 0
                limit = 10000
                while True:
                    await cur.execute(f'{sql} LIMIT {limit} OFFSET {offset}')

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

                    if cur.rowcount == 0:
                        break

                    offset += limit

    @staticmethod
    def checksum_column_normalization(value):
        if isinstance(value, (datetime, date)):
            return int(time.mktime(value.timetuple()))

        return value

    async def is_changed(self, stor: ChecksumStorage, pk: int, data: typing.Iterable) -> bool:
        checksum = sha1(':'.join(map(str, data)).encode('utf-8')).hexdigest()

        if (await stor[pk]) != checksum:
            await stor.set(pk, checksum)

            return False

        return True


class CodeAdditionalDataFetcher:

    @classmethod
    async def fetch(cls, record: dict, table: tables.TableConfig, db: aiopg.Pool):
        data = {}
        async with db.acquire() as conn:
            if record['pms_code_vetsuccess_id']:
                data['code_tags'] = await cls.fetch_code_tags(record, table, conn)
            if record['revenue_category_id']:
                data['revenue_category'] = await cls.fetch_revenue_category(record, table, conn)

        return data

    @staticmethod
    async def fetch_code_tags(record: dict, table: tables.TableConfig, conn: aiopg.Connection) -> list:
        code_tags = []

        async with conn.cursor() as cur:
            await cur.execute(table.get_code_tags_sql(record['pms_code_vetsuccess_id']))

            column_names = [
                column.name
                for column in cur.description
            ]

            async for rawdata in cur:
                code_tags.append(dict(zip(column_names, rawdata)))

            if code_tags:
                ids = set(itertools.chain(*[
                    code_tag['ancestry'].split('/')
                    for code_tag in code_tags
                ]))
                await cur.execute(table.get_related_code_tags_sql(ids))
                async for rawdata in cur:
                    data = dict(zip(column_names, rawdata))
                    code_tags.append(data)

        return sorted(code_tags, key=lambda r: r['id'])

    @staticmethod
    async def fetch_revenue_category(record: dict, table: tables.TableConfig, conn: aiopg.Connection) -> dict:
        revenue_category = None
        async with conn.cursor() as cur:
            for search_field in ('revenue_category_id', 'subset_of_level_2_id', 'subset_of_level_1_id'):
                await cur.execute(table.get_revenue_category_sql(
                    search_field,
                    record['revenue_category_id']
                ))

                rawdata = await cur.fetchone()
                if rawdata:
                    column_names = [
                        column.name
                        for column in cur.description
                    ]
                    revenue_category = dict(zip(column_names, rawdata))

                    break

        return revenue_category


async def connect(dsn: str, redis: aioredis.ConnectionsPool) -> DB:
    pool = await aiopg.create_pool(dsn, enable_hstore=False)

    return DB(pool, redis)
