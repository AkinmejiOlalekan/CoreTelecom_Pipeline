WITH stage AS (
    SELECT *
    FROM {{ source("staging", "call_logs") }}
)

SELECT *
FROM stage