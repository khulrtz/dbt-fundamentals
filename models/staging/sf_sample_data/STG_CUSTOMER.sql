WITH

SRC_TAB AS
(
    SELECT * FROM {{ source('ingestas', 'customer') }}
)

SELECT * FROM SRC_TAB