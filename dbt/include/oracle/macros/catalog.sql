
{% macro oracle__get_catalog(information_schema, schemas) -%}
  {% set query %}
    select 
        '{{ information_schema.database }}' as "table_database",
        owner as "table_schema",
        table_name as "table_name",
        -- Hacky. Oracle does not collect info for views so use that as table indicator.
        case
            when num_distinct is null then 'view'
            else 'table'
        end as "table_type",
        null as "table_comment",  -- We can get that with an extra join. Leave it for later.
        owner as "table_owner",

        column_name as "column_name",
        column_id as "column_index",
        data_type as "column_type",
        null as "column_comment"  -- Can we even get this?

    from all_tab_columns
    where upper(owner) in
        (
        {%- for schema in schemas -%}
          upper('{{ schema }}'){%- if not loop.last %},{% endif -%}
        {%- endfor -%}
        )
    order by owner, table_name, column_id
  {%- endset -%}

  {{ return(run_query(query)) }}
{% endmacro %}
