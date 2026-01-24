{% macro col_or_null(source_name, table_name, column_name, data_type='text') %}
  {%- set rel = source(source_name, table_name) -%}
  {%- set cols = adapter.get_columns_in_relation(rel) -%}
  {%- set colnames = cols | map(attribute='name') | list -%}

  {%- if column_name in colnames -%}
    {{ adapter.quote(column_name) }}
  {%- else -%}
    null::{{ data_type }}
  {%- endif -%}
{% endmacro %}
