{{ config(materialized='table') }}

with final as (
    select *
    from {{ ref("bank_transfer_payments") }}

    union all

    select *
    from {{ ref("coupon_payments") }}

    union all

    select *
    from {{ ref("credit_card_payments") }}
)

select *
from final
where amount > 1000
