{% macro pivotar(column_name, table_name, met_pattern='NUM') -%}

{%- set values_query -%}
select distinct {{column_name}} from {{table_name}}
{%- endset -%}

{%- set results = run_query(values_query) -%}

{%- if execute -%}
{%- set results_list = results.columns[0].values() -%}
{%- else -%}
{%- set results_list = [] -%}
{%- endif -%}
{% for i in results_list -%}
    SUM(CASE WHEN {{column_name}} = '{{ i }}' THEN 1 ELSE 0 END) AS {{met_pattern}}_{{ i }}
    {%- if not loop.last -%}
    ,
    {% endif %}
{%- endfor %}
{%- endmacro %}