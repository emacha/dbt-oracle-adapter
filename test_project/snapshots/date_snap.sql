{% snapshot cc_date_snapshot %}
    {{ config(
        check_cols=['some_date'], unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
