/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view'
,pre_hook="select count(1) from {{ ref('python_test') }}") }}


with source_data as (
    select
    *
    from {{ ref('python_test') }}
)

select  *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
