
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with final as (

    select id
    from {{ ref('1_5') }}
    union 
    select id
    from {{ ref('1_6') }}    

)

select *
from final

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
