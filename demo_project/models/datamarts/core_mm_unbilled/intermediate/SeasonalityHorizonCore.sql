/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='ephemeral') }}


WITH source_data AS (

    SELECT DISTINCT
        year,
        month AS mth_no,
        first_day_month AS mth_sdt,
        last_day_month AS mth_edt,
        dayofmonth(last_day(date)) AS mth_days
    FROM
        {{ source('dm_common', 'dim_date') }}
    WHERE
        date_add(last_day(add_months(date, -1)), 1) BETWEEN date_add(
            last_day(
                add_months(
                    last_day(
                        add_months(
                            current_date(), -{{ var('SeasonalityHorizonMonths',24) }} - 5
                        )
                    ),
                    -1
                )
            ),
            1
        )
        AND date_add(
            last_day(
                add_months(last_day(add_months(current_date(), -6)), -1)
            ),
            1
        )

)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
