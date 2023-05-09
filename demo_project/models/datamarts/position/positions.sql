
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view',alias='positions_all') }}

with final as (

    select id
    from {{ ref('position_1') }}    
    union 
    select id
    from {{ ref('position_2') }}  
    union     
    select id
    from {{ ref('position_3') }}  
    union     
    select id
    from {{ ref('position_4') }}  
    union     
    select id
    from {{ ref('position_5') }}       

)

select *
from final

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
