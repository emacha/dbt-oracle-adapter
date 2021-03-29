{{ config(materialized='ephemeral') }}
select *
from {{ source('dbt_test_user', 'raw_payments') }}
