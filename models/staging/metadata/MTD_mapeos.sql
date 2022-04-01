
WITH

mapeos as (

    select
    'ingestas' as DB_ORIGEN,
    TB_ORIGEN,
    CL_ORIGEN,
    'dbtfundamentals' as DB_DESTINO,
    TABLA_DESTINO,
    CL_DESTINO,
    IN_HASH,
    IN_TRIM,
    IN_TS_SCD,
    IN_KEY
    from {{ ref('metadata') }}
)

select * from mapeos where TB_ORIGEN IN ('CUSTOMER','ORDERS')