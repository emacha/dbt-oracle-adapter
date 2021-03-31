{% macro oracle__create_table_as(temporary, relation, sql) -%}
  {% if temporary: -%}
  create global temporary table {{ relation }}
  on commit preserve rows
  {% else %}
  create table {{ relation }}
  {% endif %}
  as 
    {{ sql }}
{% endmacro %}
