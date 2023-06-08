/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='ephemeral',pre_hook="SET spark.sql.parquet.compression.codec=gzip;") }}


with source_data as (

    select distinct
        year,
        month as mth_no,
        first_day_month as mth_sdt,
        last_day_month as mth_edt,
        dayofmonth(last_day(date)) as mth_days
    from {{ source('dm_common', 'dim_date') }}
    where
        date_add(
            last_day(add_months(date, -1)), 1
        ) between '{{ var('Month_Start_Date','2022-04-01T00:00:00') }}' and '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
