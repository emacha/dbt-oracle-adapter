from dbt.adapters.sql import SQLAdapter

from dbt.adapters.noracle import NoracleConnectionManager


class NoracleAdapter(SQLAdapter):
    ConnectionManager = NoracleConnectionManager

    @classmethod
    def date_function(cls):
        return "select current_date from dual"

    def debug_query(self) -> None:
        self.execute("select 1 as id from dual")
