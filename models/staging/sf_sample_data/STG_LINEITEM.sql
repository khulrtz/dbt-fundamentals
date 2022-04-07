WITH

SRC_TAB AS
(
    SELECT * FROM {{ source('ingestas', 'lineitem') }}
)

SELECT * FROM SRC_TAB