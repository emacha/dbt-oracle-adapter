select amount
from {{ ref('large_payments' )}}
where amount <= 1000
