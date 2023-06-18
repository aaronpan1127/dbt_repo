/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view',alias='positions_all') }}

WITH final AS (

    SELECT id
    FROM {{ ref('position_1') }}
    UNION
    SELECT id
    FROM {{ ref('position_2') }}
    UNION
    SELECT id
    FROM {{ ref('position_3') }}
    UNION
    SELECT id
    FROM {{ ref('position_4') }}
    UNION
    SELECT id
    FROM {{ ref('position_5') }}

)

SELECT *
FROM final

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
