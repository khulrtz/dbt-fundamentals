WITH

SRC_TAB AS
(
    SELECT * FROM {{ source('ingestas', 'defaultvalue') }}
)

SELECT * FROM SRC_TAB