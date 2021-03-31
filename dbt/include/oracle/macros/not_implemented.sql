{# We'll override default macros and cause them to raise
a compiler error. 
Oracle is different enough from the usual
dbt database that the default implemetations can do more
harm than good. 
This way at least the error message points us towards where
the problem is.
#}

{% macro oracle__alter_column_type(relation) -%}
  {% set msg -%}
    alter_column_type not implemented for oracle
  {%- endset %}
  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}
