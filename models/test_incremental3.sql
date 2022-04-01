{{
    config(
        materialized='incremental',
        file_format='delta',
        unique_key='ID',
        incremental_strategy='merge',
        partition_by='VAL2,VAL1',
        pre_hook=['SET spark.sql.shuffle.partitions=2'],
        post_hook=['OPTIMIZE dbtfundamentals.test_incremental3 ZORDER BY (ID)']
    )
}}

WITH

test_data as (
    
    SELECT 1 AS ID, 'A' AS VAL1, 'A' AS VAL2 UNION 
    SELECT 2 AS ID, 'B' AS VAL1, 'B' AS VAL2 UNION 
    SELECT 3 AS ID, 'C' AS VAL1, 'A' AS VAL2 UNION 
    SELECT 4 AS ID, 'D' AS VAL1, 'B' AS VAL2 UNION 
    SELECT 5 AS ID, 'E' AS VAL1, 'A' AS VAL2 UNION
    SELECT 6 AS ID, 'F' AS VAL1, 'D' AS VAL2 UNION
    SELECT 7 AS ID, 'F' AS VAL1, 'M' AS VAL2
)

SELECT * FROM test_data;