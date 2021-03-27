from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import Policy


@dataclass(frozen=True, eq=False, repr=False)
class NoracleRelation(BaseRelation):
    include_policy: Policy = Policy(database=False)
    quote_policy: Policy = Policy(database=False, schema=False, identifier=False)
