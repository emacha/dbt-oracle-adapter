
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


{% macro noracle__rename_relation(from_relation, to_relation) -%}
  {% if from_relation.type != to_relation.type -%}
  {{ exceptions.raise_compiler_error('Cannot rename view into tables or vice-versa') }}
  {% endif %}

  {% if from_relation.type == 'view' -%}
    {# We cannot rename view in other schemas with Oracle. So we need to drop and recreate it. #}}
    {%- call statement('get_view_DDL', fetch_result=true) %}
      select text 
      from all_views 
      where owner = upper('{{ from_relation.schema }}')
        and view_name = upper('{{ from_relation.identifier }}')
    {%- endcall -%}

    {% set sql -%}
      create or replace view {{to_relation}} as
      {{ load_result('get_view_DDL')['data'][0][0] }}
    {%- endset %}
    {% do run_query(sql) %}

    {% set sql -%}
      drop view {{from_relation}}
    {%- endset %}
    {% do run_query(sql) %}

  {% elif from_relation.type == 'table' -%}
    {% call statement('rename_relation') -%}
      alter table {{from_relation}} rename to {{to_relation.identifier}}
    {%- endcall %}
  {%- else -%}
  {{ exceptions.raise_compiler_error('Rename not implemented for {{from_relation.type}}') }}
  {% endif %}
{% endmacro %}


{% macro noracle__truncate_relation(relation) -%}
  {% call statement('truncate_relation', auto_begin=False) -%}
    truncate table {{relation.include(database=false)}}
  {%- endcall %}
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


{% macro noracle__drop_schema(database_name, schema_name) -%}
  {% set typename = adapter.type() %}

  {%- call statement('drop_schema', fetch_result=False) -%}
  drop user {{database_name}} CASCADE
  {%- endcall -%}

{% endmacro %}


{% macro noracle__create_schema(database_name, schema_name) -%}
  {% set typename = adapter.type() %}
  {% set grant_types = ["create session", "create table", "create view", "create any trigger", "create any procedure", "create sequence",
                        "create synonym", "unlimited tablespace"] %}

  {%- call statement('create_user', fetch_result=False) -%}
    create user {{database_name}}
    identified by 1234
    default tablespace USERS
    temporary tablespace TEMP
  {%- endcall -%}

  {% for grant_type in grant_types %}
    {%- call statement('add_grant', fetch_result=False) -%}
      grant {{grant_type}} to {{database_name}}
  {%- endcall -%}
  {% endfor %}
{% endmacro %}


{% macro noracle__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    select count(*) from sys.all_users where username = upper('{{ schema }}')
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro noracle__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
  select
    column_name as "column_name",
    data_type as "data_type"
  from all_tab_columns
  where owner = upper('{{ relation.schema }}')
    and table_name = upper('{{ relation.identifier }}')
  {% endcall %}

  {% set result = load_result('get_columns_in_relation').table.rows %}}
  {% set columns = [] %}
  {% for row in result %}
    {% do columns.append(api.Column.from_description(row['column_name'], row['data_type'])) %}
  {% endfor %}
  {% do return(columns) %}

{% endmacro %}
