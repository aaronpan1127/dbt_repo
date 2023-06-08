/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



{{ config(materialized='table',alias='seasonalavgnbcore',post_hook='analyze table {{ this }} compute statistics') }}

with t as (
    select
        b.fuel_type,
        b.market_region,
        b.market_segment,
        b.customer_segment,
        b.market_sub_region,
        md5(
            concat(
                date_part('MONTH', s.mth_sdt),
                market_sub_region,
                fuel_type,
                market_region,
                customer_segment
            )
        ) as record_hash,
        date_part('MONTH', s.mth_sdt) as mth_no,
        count(distinct seq_product_item_id) as record_count
    from
        {{ ref('RetailInvoiceCORE') }} as b
    cross join
        {{ ref('SeasonalityHorizonCore') }} as s
    where
        b.bill_sdt <= s.mth_edt
        and b.bill_edt >= s.mth_sdt
        and measure_name is not null
    group by
        b.fuel_type,
        b.market_region,
        b.market_segment,
        b.customer_segment,
        b.market_sub_region,
        date_part('MONTH', s.mth_sdt)

),
source_data as (
    select
        a.*,
        t.record_count
    from
        (
            select distinct
                b.fuel_type,
                b.market_region,
                b.market_segment,
                b.customer_segment,
                b.market_sub_region,
                b.cntrl_load,
                b.solar_zr,
                b.ex_charge_desc as trans_description,
                b.measure_code,
                b.measure_name,
                b.measure_unit,
                md5(
                    concat(
                        date_part('MONTH', s.mth_sdt),
                        market_sub_region,
                        fuel_type,
                        market_region,
                        customer_segment
                    )
                ) as record_hash,
                date_part('MONTH', s.mth_sdt) as mth_no,
                round(avg(b.daily_unit_quantity), 5) as avg_daily_unit_quantity,
                round(avg(b.daily_unit_quantity_zr), 5)
                    as avg_daily_unit_quantity_zr,
                round(avg(b.daily_amt), 5) as avg_daily_amt,
                round(avg(b.daily_disc_amt), 5) as avg_daily_disc_amt,
                round(avg(b.daily_ppd_amt), 5) as avg_daily_ppd_amt,
                round(avg(b.daily_elig_ppd_amt), 5) as avg_daily_elig_ppd_amt,
                count(distinct b.seq_product_item_id) as account_number_count
            from
                {{ ref('RetailInvoiceCORE') }} as b
            cross join
                {{ ref('SeasonalityHorizonCore') }} as s
            where
                b.bill_sdt <= s.mth_edt
                and b.bill_edt >= s.mth_sdt
                and measure_name is not null
            group by
                b.fuel_type,
                b.market_region,
                b.market_segment,
                b.customer_segment,
                b.market_sub_region,
                b.cntrl_load,
                b.solar_zr,
                b.ex_charge_desc,
                b.measure_code,
                b.measure_name,
                b.measure_unit,
                date_part('MONTH', s.mth_sdt)

        ) as a
    inner join

        t
        on a.record_hash = t.record_hash
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
