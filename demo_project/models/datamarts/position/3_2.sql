/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

WITH final AS (

    SELECT id
    FROM {{ ref('2_4') }}
    UNION
    SELECT id
    FROM {{ ref('2_5') }}
    UNION
    SELECT id
    FROM {{ ref('2_6') }}
    UNION
    SELECT id
    FROM {{ ref('2_7') }}

)

SELECT *
FROM final

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
