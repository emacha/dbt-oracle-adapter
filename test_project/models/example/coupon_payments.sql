{{ config(materialized='view') }}

select *
from {{ source('dbt_test_user', 'raw_payments') }}
where payment_method = 'coupon'
