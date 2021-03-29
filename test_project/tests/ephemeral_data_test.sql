select amount
from {{ ref('ephemeral_recurse' )}}
where amount < 0
