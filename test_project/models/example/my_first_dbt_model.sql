{{ config(materialized='table') }}

with source_data as (

    select 1 as id from dual
    union all
    select null as id from dual

)

select *
from source_data
