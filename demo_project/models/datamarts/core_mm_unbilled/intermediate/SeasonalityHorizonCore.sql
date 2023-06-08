/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='ephemeral') }}


with source_data as (

    select distinct
        year,
        month as mth_no,
        first_day_month as mth_sdt,
        last_day_month as mth_edt,
        dayofmonth(last_day(date)) as mth_days
    from
        {{ source('dm_common', 'dim_date') }}
    where
        date_add(last_day(add_months(date, -1)), 1) between date_add(
            last_day(
                add_months(
                    last_day(
                        add_months(
                            current_date(),-{{ var('SeasonalityHorizonMonths',24) }} - 5
                        )
                    ),
                    -1
                )
            ),
            1
        )
        and date_add(
            last_day(
                add_months(last_day(add_months(current_date(),-6)), -1)
            ),
            1
        )

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
