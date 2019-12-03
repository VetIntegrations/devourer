from devourer.core.datasource import exceptions


class ImproperTableConfig(exceptions.DataSourceException):
    message = 'Table config should have timestamp or checksum column'


class TableConfig:

    def __init__(self, name: str, timestamp_column: str = None, checksum_column: str = None, order_by: str = 'id'):
        if not any((timestamp_column, checksum_column)):
            raise ImproperTableConfig()

        self.name = name
        self.timestamp_column = timestamp_column
        self.checksum_column = checksum_column
        self.order_by = order_by

    def get_sql(self) -> str:
        sql = None
        if self.timestamp_column:
            sql = (
                f'SELECT * FROM external.{self.name} '
                f"WHERE {self.timestamp_column} >= '%(timestamp)s'::timestamp "
                f'ORDER BY {self.order_by} '
            )
        else:
            sql = (
                f'SELECT * FROM external.{self.name} '
                f'ORDER BY {self.order_by} '
            )

        return sql


class PatientTableConfig(TableConfig):

    def get_sql(self) -> str:
        return (
            f'SELECT DISTINCT {self.name}.vetsuccess_id, rt.client_vetsuccess_id, {self.name}.* '
            f'FROM external.{self.name} '
            f'INNER JOIN external.revenue_transactions as rt ON rt.patient_vetsuccess_id = {self.name}.vetsuccess_id '
            f'ORDER BY {self.order_by} '
        )
