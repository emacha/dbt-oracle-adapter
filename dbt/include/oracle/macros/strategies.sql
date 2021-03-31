{% macro oracle__snapshot_string_as_time(timestamp) %}
    {{ return(timestamp) }}
{% endmacro %}


{% macro oracle__snapshot_merge_sql(target, source, insert_cols) -%}
    {% set insert_cols_dest -%}
    {%- for col in insert_cols -%}
    DBT_INTERNAL_DEST.{{col}}{%- if not loop.last -%}, {%- endif -%}
    {%- endfor -%}
    {%- endset %}

    {% set insert_cols_source -%}
    {%- for col in insert_cols -%}
    DBT_INTERNAL_SOURCE.{{col}}{%- if not loop.last -%}, {%- endif -%}
    {%- endfor -%}
    {%- endset %}

    merge into {{ target }} DBT_INTERNAL_DEST
    using {{ source }} DBT_INTERNAL_SOURCE
    on (DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id)

    when matched
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    where DBT_INTERNAL_DEST.dbt_valid_to is null
      and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')

    {% if insert_cols|length > 0 %}
    when not matched
        then insert ({{ insert_cols_dest }})
        values ({{ insert_cols_source }})
    where DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
    {% endif %}
{% endmacro %}



{#
Oracle cannot take an unquoted date as a value. So even though
the these 2 predicate branches are basically indentical for column check and
timestamp we need 2 arms with the only change as
strategy.updated_at being wrapped in quotes 
timestamp -> cc
{{ strategy.updated_at }} -> '{{ strategy.updated_at }}'

This is obviouly a terrible 'solution'. The proper way to do this
would be to have the `snapshot_string_as_time` macro return
a string that would render with single-quotes. But I've spent
too long searching for how to do this with no success, and I
admit defeat. 
#}
{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}
    {% if 'is_cc_strat' in strategy %}
        with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select {{ target_relation }}.*,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            '{{ strategy.updated_at }}' as dbt_updated_at,
            '{{ strategy.updated_at }}' as dbt_valid_from,
            nullif('{{ strategy.updated_at }}', '{{ strategy.updated_at }}') as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            '{{ strategy.updated_at }}' as dbt_updated_at,
            '{{ strategy.updated_at }}' as dbt_valid_from,
            '{{ strategy.updated_at }}' as dbt_valid_to

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select 
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (
    
        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            {{ snapshot_get_time() }} as dbt_valid_to,
            snapshotted_data.dbt_scd_id
    
        from snapshotted_data
        left join deletes_source_data source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}
    
    {% else %}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select {{ target_relation }}.*,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select 
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (
    
        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            {{ snapshot_get_time() }} as dbt_valid_to,
            snapshotted_data.dbt_scd_id
    
        from snapshotted_data
        left join deletes_source_data source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}
    {% endif %}

{%- endmacro %}


{% macro build_snapshot_table(strategy, sql) %}
    {% if 'is_cc_strat' in strategy %}
        select sbq.*,
        {{ strategy.scd_id }} as dbt_scd_id,
        '{{ strategy.updated_at }}' as dbt_updated_at,
        '{{ strategy.updated_at }}' as dbt_valid_from,
        cast(nullif('{{ strategy.updated_at }}', '{{ strategy.updated_at }}') as varchar2(1000)) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq
    {% else %}
    select sbq.*,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq
    {% endif %}
{% endmacro %}


{% macro oracle__snapshot_hash_arguments(args) -%}
    standard_hash({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar2(1000) ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%}, 'MD5')
{%- endmacro %}


{% macro snapshot_check_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    
    {% set select_current_time -%}
        select '{{ snapshot_get_time() }}' as snapshot_start from dual 
    {%- endset %}

    {#-- don't access the column by name, to avoid dealing with casing issues on snowflake #}
    {%- set now = run_query(select_current_time)[0][0] -%}
    {% if now is none or now is undefined -%}
        {%- do exceptions.raise_compiler_error('Could not get a snapshot start time from the database') -%}
    {%- endif %}
    {% set updated_at = snapshot_string_as_time(now) %}

    {% set column_added = false %}

    {% if check_cols_config == 'all' %}
        {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists) %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {%- set row_changed_expr -%}
    (
    {%- if column_added -%}
        TRUE
    {%- else -%}
    {%- for col in check_cols -%}
        {{ snapshotted_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
        or
        (
            (({{ snapshotted_rel }}.{{ col }} is null) and not ({{ current_rel }}.{{ col }} is null))
            or
            ((not {{ snapshotted_rel }}.{{ col }} is null) and ({{ current_rel }}.{{ col }} is null))
        )
        {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    {%- endif -%}
    )
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}
    {% set scd_id_expr %}
        standard_hash(coalesce(cast({{ primary_key }} as varchar2(1000)), '')
        || '|' || coalesce(cast('{{ updated_at }}' as varchar2(1000)), ''), 'MD5')
    {% endset %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "is_cc_strat": True
    }) %}
{% endmacro %}
