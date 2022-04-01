WITH

SRC_TAB AS
(
    SELECT * FROM {{ source('ingestas', 'region') }}
)

SELECT * FROM SRC_TAB