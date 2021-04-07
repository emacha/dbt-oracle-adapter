from typing import Dict

import agate
from dbt.adapters.sql import SQLAdapter
from dbt.utils import filter_null_values

from dbt.adapters.oracle import OracleConnectionManager
from dbt.adapters.oracle.relation import OracleRelation


class OracleAdapter(SQLAdapter):
    ConnectionManager = OracleConnectionManager
    Relation = OracleRelation

    @classmethod
    def date_function(cls):
        return "select current_date from dual"

    def debug_query(self) -> None:
        self.execute("select 1 as id from dual")

    def _make_match_kwargs(
        self, database: str, schema: str, identifier: str
    ) -> Dict[str, str]:
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.upper()

        if schema is not None and quoting["schema"] is False:
            schema = schema.upper()

        if database is not None and quoting["database"] is False:
            database = database.upper()

        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
                "schema": schema,
            }
        )

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "varchar(4000)"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "number"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "number(1)"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp with time zone"

    def get_rows_different_sql(
        self,
        relation_a,
        relation_b,
        column_names=None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql

    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = "hour"
    ) -> str:
        if interval != "hour":
            raise NotImplementedError(
                f"Interval of type {interval} not allowed, use 'hour'"
            )

        return f"{add_to} + interval '{number}' {interval}"


COLUMNS_EQUAL_SQL = """
with simmetric_difference as (
    select {columns} from {relation_a}
    minus
    select {columns} from {relation_b}

    union all

    select {columns} from {relation_b}
    minus
    select {columns} from {relation_a}
),
table_a as (
    select count(*) as num_rows from {relation_a}
),
table_b as (
    select count(*) as num_rows from {relation_b}
),
row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
),
diff_count as (
    select
        1 as id,
        (select count(*) from simmetric_difference) as num_missing
    from dual
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
inner join diff_count
    using (id)
""".strip()
