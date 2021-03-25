
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

  {% elif from_relation.type == 'table'%}
    {% call statement('rename_relation') -%}
      alter table {{from_relation.schema}}.{{from_relation.identifier}}
      rename to {{to_relation.identifier}}
    {%- endcall %}
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

-- Macro to override ref and to render identifiers without a database.
{% macro ref(model_name) %}
  {% do return(builtins.ref(model_name).include(database=false)) %}
{% endmacro %}


{% macro noracle__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    create table {{this.include(database=false)}} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}

{% macro noracle__load_csv_rows(model, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}
    {% set batch_size = 1 %}}

    {% set statements = [] %}

    {% set sql %}
      insert into {{ this.include(database=false) }} ({{ cols_sql }}) values
      (
      {%- for column in agate_table.column_names -%}
        :{{loop.index0}}
        {%- if not loop.last%},{%- endif %}
      {%- endfor %})
    {% endset %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% do bindings.extend(row) %}
        {% endfor %}

        {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}
