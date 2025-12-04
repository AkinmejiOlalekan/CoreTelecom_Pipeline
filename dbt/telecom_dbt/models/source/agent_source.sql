WITH stage AS (
    SELECT *
    FROM {{ source("staging", "agents") }}
)

SELECT *
FROM stage