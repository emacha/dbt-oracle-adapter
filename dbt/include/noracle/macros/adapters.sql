
{% macro noracle__get_columns_in_relation(relation) -%}
  {% set msg -%}
    get_columns_in_relation not implemented for noracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}

{% macro noracle__alter_column_type(relation) -%}
  {% set msg -%}
    get_columns_in_relation not implemented for noracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}

{% macro noracle__check_schema_exists(relation) -%}
  {% set msg -%}
    get_columns_in_relation not implemented for noracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}

{% macro noracle__create_schema(relation) -%}
  {% set msg -%}
    get_columns_in_relation not implemented for noracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}

{% macro noracle__drop_relation(relation) -%}
  {% if relation.type == 'view' -%}
    {% call statement('drop_relation', auto_begin=False) -%}
      BEGIN
        EXECUTE IMMEDIATE 'drop view ' || '{{relation.name}}';
      EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
              RAISE;
            END IF;
      END;
    {%- endcall %}
   {% elif relation.type == 'table'%}
    {% call statement('drop_relation', auto_begin=False) -%}
      BEGIN
        EXECUTE IMMEDIATE 'drop table ' || '{{relation.name}}';
      EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
              RAISE;
            END IF;
      END;
    {%- endcall %}
   {%- else -%} invalid target name
   {% endif %}
{% endmacro %}

{% macro noracle__drop_schema(relation) -%}
  {% set msg -%}
    drop_schema not implemented for noracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}

{% macro noracle__rename_relation(from_relation, to_relation) -%}
  {% if from_relation.type == 'view' -%}
    {% set msg -%}
      rename view not implemented yet
    {%- endset %}
    {{ exceptions.raise_compiler_error(msg) }}
  {% elif from_relation.type == 'table'%}
    {% call statement('rename_relation') -%}
      alter table {{from_relation.schema}}.{{from_relation.identifier}}
      rename to {{to_relation.identifier}}
    {%- endcall %}
  {% endif %}
{% endmacro %}

{% macro noracle__truncate_relation(relation) -%}
  {% set msg -%}
    truncate_relation not implemented for noracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}


{% macro noracle__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True, auto_begin=False) -%}
    select
    upper('{{schema_relation.database}}') as database,
    table_name as name,
    upper('{{schema_relation.schema}}') as schema,
    'table' as table_type
    from all_tables
    {# We're doing a case insensitive search in here! #}
    where upper(owner) = upper('{{schema_relation.schema}}')

    union all

    select
        upper('{{schema_relation.database}}') as database,
        view_name as name,
        upper('{{schema_relation.schema}}') as schema,
        'view' as table_type
    from all_views
    where upper(owner) = upper('{{schema_relation.schema}}')
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

{% macro noracle__create_table_as(temporary, relation, sql) -%}
  create {% if temporary: -%}global temporary{%- endif %} table
    {{ relation.include(database=false, schema=(not temporary)) }}
  as 
    {{ sql }}
{% endmacro %}

{% macro noracle__create_view_as(relation, sql) -%}
  create view {{ relation.include(database=false, schema=true) }} as
    {{ sql }}
{% endmacro %}
