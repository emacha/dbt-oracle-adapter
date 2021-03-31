
{% macro oracle__test_relationships(model, to, field) %}

{% set column_name = kwargs.get('column_name', kwargs.get('from')) %}


{# This is tightly coupled to how the compiler injects `model` into the query. #}
,
child as (
    select {{ column_name }} as id from {{ model }}
),
parent as (
    select {{ field }} as id from {{ to }}
),
final as (
    select *
    from child
    left join parent
        on parent.id = child.id
    where child.id is not null
      and parent.id is null
)
select count(*) as validation_errors from final

{% endmacro %}
