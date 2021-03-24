from typing import Dict
from dbt.adapters.sql import SQLAdapter
from dbt.utils import filter_null_values
from dbt.adapters.noracle import NoracleConnectionManager


class NoracleAdapter(SQLAdapter):
    ConnectionManager = NoracleConnectionManager

    @classmethod
    def date_function(cls):
        return "select current_date from dual"

    def debug_query(self) -> None:
        self.execute("select 1 as id from dual")

    def _make_match_kwargs(
        self, database: str, schema: str, identifier: str
    ) -> Dict[str, str]:
        quoting = self.config.quoting
        if identifier is not None and quoting['identifier'] is False:
            identifier = identifier.upper()

        if schema is not None and quoting['schema'] is False:
            schema = schema.upper()

        if database is not None and quoting['database'] is False:
            database = database.upper()

        return filter_null_values({
            'database': database,
            'identifier': identifier,
            'schema': schema,
        })
