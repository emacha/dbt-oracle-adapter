{% macro noracle__create_view_as(relation, sql) -%}
  create view {{ relation.render() }} as
    {{ sql }}
{% endmacro %}
