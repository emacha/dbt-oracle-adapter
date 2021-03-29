{{ config(materialized='ephemeral') }}
select *
from {{ ref("credit_card_payments") }}
