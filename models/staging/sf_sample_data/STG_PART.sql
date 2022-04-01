WITH

SRC_TAB AS
(
    SELECT * FROM {{ source('ingestas', 'part') }}
)

SELECT * FROM SRC_TAB