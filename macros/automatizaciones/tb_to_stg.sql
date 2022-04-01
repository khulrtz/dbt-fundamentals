{% macro tb_to_stg(args) %}
    SELECT * FROM {{ source('source', 'table_name') }}
{% endmacro %}