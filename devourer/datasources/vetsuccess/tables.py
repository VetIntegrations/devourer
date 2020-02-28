import typing

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
            f'SELECT DISTINCT {self.name}.vetsuccess_id, rel.client_vetsuccess_id, {self.name}.* '
            f'FROM external.{self.name} '
            f'INNER JOIN external.client_patient_relationships as rel ON '
            f'  rel.patient_vetsuccess_id = {self.name}.vetsuccess_id AND rel.is_primary = \'true\' '
            f'ORDER BY {self.order_by} '
        )


class PatientCoOwnerTableConfig(TableConfig):

    def get_sql(self) -> str:
        return (
            f'SELECT {self.name}.* FROM external.{self.name} '
            f'WHERE is_primary = \'false\' ORDER BY {self.order_by} '
        )


class CodeTableConfig(TableConfig):

    def get_code_tags_sql(self, pms_code_vetsuccess_id: str) -> str:
        return (
            'SELECT '
            '  code_tags.*, '
            '  code_tag_mappings.pms_code_vetsuccess_id, '
            ' code_tag_mappings.practice_id '
            'FROM external.code_tags '
            'LEFT OUTER JOIN external.code_tag_mappings ON code_tag_mappings.code_tag_id = code_tags.id '
            'WHERE code_tag_mappings.pms_code_vetsuccess_id = \'{pk}\''
        ).format(pk=pms_code_vetsuccess_id)

    def get_related_code_tags_sql(self, ids: typing.List[str]) -> str:
        return (
            'SELECT '
            '  code_tags.*, '
            '  code_tag_mappings.pms_code_vetsuccess_id, '
            '  code_tag_mappings.practice_id '
            'FROM external.code_tags '
            'LEFT OUTER JOIN external.code_tag_mappings ON code_tag_mappings.code_tag_id = code_tags.id '
            'WHERE code_tags.id = ANY(ARRAY[{ids}]); '
        ).format(ids=', '.join(ids))

    def get_revenue_category_sql(self, field, revenue_category_id: int) -> str:
        return (
            'SELECT * FROM external.revenue_categories_hierarchy '
            'WHERE {field}={pk} '
        ).format(field=field, pk=revenue_category_id)
