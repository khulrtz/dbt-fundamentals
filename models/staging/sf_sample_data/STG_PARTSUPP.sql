WITH

SRC_TAB AS
(
    SELECT * FROM {{ source('ingestas', 'partsupp') }}
)

SELECT * FROM SRC_TAB