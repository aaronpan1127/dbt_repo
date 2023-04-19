{{ config(materialized='table',location_root='/mnt/source_image/test') }}

-- Use the `ref` function to select from other models

select *
from {{ ref('my_first_dbt_model') }}
where id = 1
