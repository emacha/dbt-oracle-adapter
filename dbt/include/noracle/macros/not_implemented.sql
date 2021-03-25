{# We'll override default macros and cause them to raise
a compiler error. 
Oracle is different enough from the usual
dbt database that the default implemetations can do more
harm than good. 
This way at least the error message points us towards where
the problem is.
#}

{% macro noracle__drop_schema(relation) -%}
  {% set msg -%}
    drop_schema not implemented for noracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}


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
