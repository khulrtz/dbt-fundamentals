WITH

SRC_TAB AS
(
    SELECT * FROM {{ source('ingestas', 'supplier') }}
)

SELECT * FROM SRC_TAB