
select *
from {{ source('dbt_testing', 'raw_payments') }}
where payment_method = 'credit_card'
