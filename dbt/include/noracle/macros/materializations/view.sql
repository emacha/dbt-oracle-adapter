{% macro noracle__create_view_as(relation, sql) -%}
  create view {{ relation.include(database=false, schema=true) }} as
    {{ sql }}
{% endmacro %}
