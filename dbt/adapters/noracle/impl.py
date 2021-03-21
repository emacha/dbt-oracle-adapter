from dbt.adapters.sql import SQLAdapter
from dbt.adapters.noracle import NoracleConnectionManager


class NoracleAdapter(SQLAdapter):
    ConnectionManager = NoracleConnectionManager

    @classmethod
    def date_function(cls):
        raise RuntimeError("Not fully implemented yet!")
        return 'datenow()'
