/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='neverbilledcore') }}


with source_data as (
    select
        -- Creating record_hash on the seasonality profile data group
        *,
        md5(
            concat(
                mth_no,
                market_sub_region,
                fuel_type,
                market_region,
                customer_segment
            )
        ) as record_hash
    from (
        select distinct
            h.mth_no,
            h.mth_sdt,
            h.mth_edt,
            m.seq_product_item_id,
            m.fuel_type,
            m.market_region,
            m.market_sub_region,
            m.service_sdt,
            m.service_edt,
            m.customer_segment
        from {{ ref('AccrualHorizonCore') }} as h
        inner join {{ ref('NeverBilledCOREView') }} as m
            -- service period during the reporting month
            on m.service_sdt <= h.mth_edt and m.service_edt >= h.mth_sdt
    ) as tbl
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
