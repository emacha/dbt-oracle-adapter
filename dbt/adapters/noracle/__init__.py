from dbt.adapters.base import AdapterPlugin

from dbt.adapters.noracle.connections import NoracleConnectionManager  # noqa
from dbt.adapters.noracle.connections import NoracleCredentials
from dbt.adapters.noracle.impl import NoracleAdapter
from dbt.adapters.noracle.relation import NoracleRelation  # noqa: F401
from dbt.include import noracle

Plugin = AdapterPlugin(
    adapter=NoracleAdapter,
    credentials=NoracleCredentials,
    include_path=noracle.PACKAGE_PATH,
)
