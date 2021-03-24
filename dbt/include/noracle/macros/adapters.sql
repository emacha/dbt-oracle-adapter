
{% macro noracle__get_columns_in_relation(relation) -%}
  {% set msg -%}
    get_columns_in_relation not implemented for noracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}


{% macro noracle__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True, auto_begin=False) -%}
    select
    '{{schema_relation.database}}' as database,
    table_name as name,
    '{{schema_relation.schema}}' as schema,
    'table' as table_type
    from all_tables
    where owner = '{{schema_relation.schema}}'

    union all

    select
        '{{schema_relation.database}}' as database,
        view_name as name,
        '{{schema_relation.schema}}' as schema,
        'view' as table_type
    from all_views
    where owner = '{{schema_relation.schema}}'
  {% endcall %}

  {{ return(load_result('list_relations_without_caching').table) }}

{% endmacro %}


{% macro noracle__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
    select distinct username as schema_name
    from sys.all_users
  {% endcall %}

  {{ return(load_result('list_schemas').table) }}
{% endmacro %}
