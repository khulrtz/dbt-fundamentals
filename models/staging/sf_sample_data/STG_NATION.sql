WITH

SRC_TAB AS
(
    SELECT * FROM {{ source('ingestas', 'nation') }}
)

SELECT * FROM SRC_TAB