{% macro noracle__create_table_as(temporary, relation, sql) -%}
  create {% if temporary: -%}global temporary{%- endif %} table
    {{ relation.include(schema=(not temporary)) }}
  as 
    {{ sql }}
{% endmacro %}
