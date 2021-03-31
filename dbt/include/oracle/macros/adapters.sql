
{% macro oracle__drop_relation(relation) -%}
  {% if relation.type == 'view' -%}
    {% call statement('drop_relation') -%}
      BEGIN
        EXECUTE IMMEDIATE 'drop view ' || '{{relation}}';
      EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
              RAISE;
            END IF;
      END;
    {%- endcall %}
   {% elif relation.type == 'table'%}
    {% call statement('drop_relation') -%}
      BEGIN
        EXECUTE IMMEDIATE 'drop table ' || '{{relation}}';
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


{% macro oracle__rename_relation(from_relation, to_relation) -%}
  {% if from_relation.type == 'view' and to_relation.type == 'view' -%}
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
      drop view {{from_relation}} cascade constraints
    {%- endset %}
    {% do run_query(sql) %}

  {% elif from_relation.type == 'table' and to_relation.type == 'table' -%}
    {% call statement('rename_relation') -%}
      alter table {{from_relation}} rename to {{to_relation.identifier}}
    {%- endcall %}
  
  {% elif from_relation.type == 'view' and to_relation.type == 'table' -%}
    {% call statement('rename_relation') -%}
      create table {{to_relation}} as
      select * from {{from_relation}}
    {%- endcall %}

    {% call statement('drop_old') -%}
      drop view {{from_relation}}
    {%- endcall %}
  
  {% elif from_relation.type == 'table' and to_relation.type == 'view' -%}
  {{ exceptions.raise_compiler_error('Cannot rename table into view!') }}

  {%- else -%}
  {{ exceptions.raise_compiler_error('Rename not implemented for {{from_relation.type}}') }}
  {% endif %}
{% endmacro %}


{% macro oracle__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{relation.include(database=false)}}
  {%- endcall %}
{% endmacro %}


{% macro oracle__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
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


{% macro oracle__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True) -%}
    select distinct username as schema_name
    from sys.all_users
  {% endcall %}

  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro oracle__drop_schema(database_name, schema_name) -%}
  {% set typename = adapter.type() %}

  {%- call statement('drop_schema') -%}
  drop user {{database_name}} cascade
  {%- endcall -%}

{% endmacro %}


{% macro oracle__create_schema(database_name, schema_name) -%}
  {% set typename = adapter.type() %}
  {% set grant_types = ["create session", "create table", "create view", "create any trigger", "create any procedure", "create sequence",
                        "create synonym", "unlimited tablespace"] %}

  {%- call statement('create_user') -%}
    create user {{database_name}}
    identified by 1234
    default tablespace USERS
    temporary tablespace TEMP
  {%- endcall -%}

  {%- call statement('add_grant') -%}
    grant all privileges to {{database_name}} identified by 1234
  {%- endcall -%}
{% endmacro %}


{% macro oracle__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) %}
    select count(*) from sys.all_users where username = upper('{{ schema }}')
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro oracle__get_columns_in_relation(relation) -%}
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


{% macro oracle__current_timestamp() -%}
  {% call statement('current_timestamp', fetch_result=True) %}
    select localtimestamp from dual
  {% endcall %}
  {{ return(load_result('current_timestamp')['data'][0][0]) }}
{%- endmacro %}


{% macro oracle__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
          select * from (
              {{ select_sql }}
          ) dbt_sbq
          where 1=1
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}
