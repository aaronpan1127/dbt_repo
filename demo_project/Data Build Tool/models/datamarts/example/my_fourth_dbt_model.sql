{{ config(
    materialized='table',
    location_root='/mnt/source_image/test'    )
    }}

-- Use the `ref` function to select from other models

SELECT *
FROM {{ ref('my_first_dbt_model') }}
WHERE id = 1
