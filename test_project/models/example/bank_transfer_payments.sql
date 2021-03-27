{{ config(materialized='ephemeral') }}

select *
from {{ source('dbt_testing', 'raw_payments') }}
where payment_method = 'bank_transfer'
