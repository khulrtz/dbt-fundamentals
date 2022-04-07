{%- macro mapeos_to_select(db_orig,tb_orig,db_dest,tb_dest) -%}

{%- set config_query -%}
    select 
    MAX(DECODE(DB_ORIGEN,'CALCULADO',NULL,db_origen)) db_origen,
    MAX(DECODE(DB_ORIGEN,'CALCULADO',NULL,tb_origen)) tb_origen,
    MAX(db_destino)DB_DESTINO,
    MAX(tabla_destino)TB_DESTINO,
    MAX(decode(in_ts_scd,'S',CL_DESTINO,NULL)) COL_TS_SCD,
    MAX(decode(in_key,'S',CL_DESTINO,NULL)) COL_KEY_SCD
    from {{ ref('MTD_mapeos') }}
    where TABLA_DESTINO = '{{ tb_dest }}'
{%- endset -%}

{%- set config_results = run_query(config_query) -%}

{%- if execute -%}
    {%- set db_origen = config_results.columns[0].values() -%}
    {%- set tb_origen_B = config_results.columns[1].values() -%}
    {%- set db_destino = config_results.columns[2].values() -%}
    {%- set tb_destino = config_results.columns[3].values() -%}
    {%- set col_ts_scd = config_results.columns[4].values() -%}
    {%- set col_key_scd = config_results.columns[5].values() -%}
{%- else -%}
    {%- set db_origen = [] -%}
    {%- set tb_origen_B = [] -%}
    {%- set db_destino = [] -%}
    {%- set tb_destino = [] -%}
    {%- set col_ts_scd = [] -%}   
    {%- set col_key_scd = [] -%}   
{%- endif -%}

{%- set db_origen = db_origen[0] -%}
{%- set tb_origen_B = tb_origen_B[0] -%}
{%- set db_destino = db_destino[0] -%}
{%- set tb_destino = tb_destino[0] -%}
{%- set col_ts_scd = col_ts_scd[0] -%}
{%- set col_key_scd = col_key_scd[0] -%}

{%- set values_query -%}
    select distinct
    tb_origen,
    cl_origen,
    cl_destino,
    in_hash,
    in_trim
    from {{ ref('MTD_mapeos') }}
    where TABLA_DESTINO = '{{ tb_dest }}'
{%- endset -%}

{%- set results = run_query(values_query) -%}

SELECT 
{% for i in results -%}
    {%- if execute -%}
        {%- set tb_origen = results.columns[0].values() -%}
        {%- set cl_origen = results.columns[1].values() -%}
        {%- set cl_destino = results.columns[2].values() -%}
        {%- set in_hash = results.columns[3].values() -%}
        {%- set in_trim = results.columns[4].values() -%}
        {%- else -%}
        {%- set tb_origen = [] -%}
        {%- set cl_origen = [] -%}
        {%- set cl_destino = [] -%}
        {%- set in_hash = [] -%}
        {%- set in_trim = [] -%}   
    {%- endif -%}
        {%- set tb_origen = tb_origen[loop.index-1] -%}
        {%- set cl_origen = cl_origen[loop.index-1] -%}
        {%- set cl_destino = cl_destino[loop.index-1] -%}
        {%- set in_hash = in_hash[loop.index-1] -%}
        {%- set in_trim = in_trim[loop.index-1] -%}

{# INICIALIZACION #}   
    {%- set campo = cl_origen -%} 
{# IN_TRIM #}
    {%- if in_trim == 'S' -%}
        {%- set campo = literal('RTRIM(LTRIM(') ~ campo ~ literal('))') -%} 
    {%- endif -%}
{# IN_HASH #}
    {%- if in_hash == 'S' -%}
        {%- set campo = dbt_utils.surrogate_key( cl_origen ) -%} 
    {%- endif -%}
 {# CAMPOS_CALCULADOS #}     
    {%- if tb_origen == 'CALCULADO' -%}
        {% if cl_origen == 'ORIGEN' %}
            {%- set campo = literal("'") ~ db_origen  ~ literal('.') ~ tb_origen_B ~ literal("'") -%}
        {%- elif cl_origen == 'LOADED' -%}
             {%- set campo = dbt_utils.current_timestamp() -%}
        {%- endif -%} 
    {%- endif -%}
{# ALIAS #}
    {%- set campo = campo  ~ literal(' AS ') ~ cl_destino -%}
{# COMA #}
    {%- if not loop.last -%}
        {%- set campo = campo  ~ literal(',') -%}   
    {%- endif -%}
    {{ campo }}
{% endfor -%}

FROM  {{ ref(tb_orig) }}

{%- endmacro -%}