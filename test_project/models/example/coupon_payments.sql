{{ config(materialized='view') }}

select *
from {{ source('dbt_testing', 'raw_payments') }}
where payment_method = 'coupon'
