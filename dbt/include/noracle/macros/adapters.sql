
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
  {% if from_relation.schema == to_relation.schema -%}
    {% call statement('rename_relation') -%}
      rename {{from_relation.identifier}} to {{to_relation.identifier}}
    {%- endcall %}
  {%- else -%}
    {% set msg -%}
      Cannot rename views in different schemas!
    {%- endset %}
    {{ exceptions.raise_compiler_error(msg) }}
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
