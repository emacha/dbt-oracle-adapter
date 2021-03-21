from dbt.adapters.sql import SQLAdapter
from dbt.adapters.noracle import NoracleConnectionManager


class NoracleAdapter(SQLAdapter):
    ConnectionManager = NoracleConnectionManager

    @classmethod
    def date_function(cls):
        raise RuntimeError("Not fully implemented yet!")
        return 'datenow()'

    def debug_query(self) -> None:
        self.execute('select 1 as id from dual')
