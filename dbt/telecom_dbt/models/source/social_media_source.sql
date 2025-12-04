WITH stage AS (
    SELECT *
    FROM {{ source("staging", "social_media") }}
)

SELECT *
FROM stage