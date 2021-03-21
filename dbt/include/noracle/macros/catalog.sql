
{% macro noracle__get_catalog(information_schema, schemas) -%}

  {% set msg -%}
    get_catalog not implemented for noracle
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}
