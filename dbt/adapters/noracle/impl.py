from dbt.adapters.base import BaseAdapter
from dbt.adapters.noracle import NoracleConnectionManager


class NoracleAdapter(BaseAdapter):
    ConnectionManager = NoracleConnectionManager
