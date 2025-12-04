WITH stage AS (
    SELECT *
    FROM {{ source("staging", "customers") }}
)

SELECT *
FROM stage