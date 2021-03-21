
{% macro noracle__get_columns_in_relation(relation) -%}

  {% set msg -%}
    get_columns_in_relation not implemented for noracle
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}

{% endmacro %}


{% macro noracle__list_relations_without_caching(schema_relation) %}

  {% set msg -%}
    list_relations_without_caching not implemented for noracle
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}

{% endmacro %}


{% macro noracle__list_schemas(database) -%}
  {% set sql %}
    select distinct username as schema_name
    from sys.all_users
  {% endset %}
  
  {{ return(run_query(sql)) }}
{% endmacro %}
