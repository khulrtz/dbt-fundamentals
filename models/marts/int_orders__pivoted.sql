with 
orders as  (
    select * from {{ ref('stg_orders' )}}
)

{%- set status_query -%}
select distinct status from orders
{%- endset -%}

{%- set results = run_query(status_query) -%}

{%- if execute -%}
{%- set results_list = results.columns[0].values() -%}
{%- else -%}
{%- set results_list = [] -%}
{%- endif -%}

,orders_pivoted as (
SELECT 
{% for i in results_list -%}
    SUM(CASE WHEN status = '{{ i }}' THEN 1 ELSE 0 END) AS num_orders_{{ i }}
    {%- if not loop.last -%}
    ,
    {% endif %}
{%- endfor %}
FROM orders
)
select * from orders_pivoted