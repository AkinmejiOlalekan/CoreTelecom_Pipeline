WITH stage AS (
    SELECT *
    FROM {{ source("staging", "web_forms") }}
)

SELECT *
FROM stage