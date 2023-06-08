/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='calcactualsdetailcoremeter') }}


with source_data as (
    select distinct
        'Core' as data_source,
        'Actual' as record_type,
        seq_product_item_id,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        fuel_type,
        trans_description,
        cntrl_load,
        solar_zr,
        bill_sdt,
        bill_edt,
        mth_sdt,
        mth_edt,
        service_sdt,
        service_edt,
        measure_name,
        measure_code,
        measure_unit,
        total_days,
        daily_amt,
        daily_unit_quantity,
        daily_unit_quantity_zr,
        daily_disc_amt,
        daily_ppd_amt,
        daily_elig_ppd_amt,
        case
            when measure_code = 'DAY_SUPPLY' then total_days
            when
                measure_code like 'VOL%'
                then CAST(mth_qty as NUMERIC(28, 15)) / 1000
            when measure_code like 'DIS%' then CAST(discount as NUMERIC(28, 15))
            when
                measure_code like 'PPD%'
                then CAST(ppd_amount as NUMERIC(28, 15))
            when
                measure_code like 'EPPD%'
                then CAST(elig_ppd_amount as NUMERIC(28, 15))
            when measure_code like 'REV%' then CAST(amount as NUMERIC(28, 15))
        end as measure
    from
        (
            select
                a.seq_product_item_id,
                a.market_region,
                a.market_sub_region,
                a.market_segment,
                a.customer_segment,
                a.fuel_type,
                a.trans_description,
                a.cntrl_load,
                a.solar_zr,
                a.bill_sdt,
                a.bill_edt,
                a.mth_sdt,
                a.mth_edt,
                a.service_sdt,
                a.service_edt,
                a.measure_name,
                a.measure_code,
                a.measure_unit,
                a.in_mth_days as total_days,
                a.daily_amt,
                a.daily_unit_quantity,
                a.daily_unit_quantity_zr,
                a.daily_disc_amt,
                a.daily_ppd_amt,
                a.daily_elig_ppd_amt,
                case
                    when a.solar_zr = 'Y' then a.daily_unit_quantity_zr else
                        a.daily_unit_quantity
                end
                * a.in_mth_days as mth_qty,
                case when a.solar_zr = 'Y' then 0 else a.daily_amt end
                * a.in_mth_days as amount,
                case when a.solar_zr = 'Y' then 0 else a.daily_disc_amt end
                * a.in_mth_days as discount,
                case when a.solar_zr = 'Y' then 0 else a.daily_ppd_amt end
                * a.in_mth_days as ppd_amount,
                case when a.solar_zr = 'Y' then 0 else a.daily_elig_ppd_amt end
                * a.in_mth_days as elig_ppd_amount
            from
                (
                    select distinct
                        b.seq_product_item_id,
                        b.market_region,
                        b.market_sub_region,
                        b.market_segment,
                        b.customer_segment,
                        b.fuel_type,
                        b.trans_description,
                        b.cntrl_load,
                        b.solar_zr,
                        b.bill_sdt,
                        b.bill_edt,
                        a.mth_sdt,
                        a.mth_edt,
                        b.service_sdt,
                        b.service_edt,
                        b.measure_name,
                        b.measure_code,
                        b.measure_unit,
                        b.daily_amt,
                        b.daily_unit_quantity,
                        b.daily_unit_quantity_zr,
                        b.daily_disc_amt,
                        b.daily_ppd_amt,
                        b.daily_elig_ppd_amt,
                        DATEDIFF(
                            case
                                when a.mth_edt < b.bill_edt then a.mth_edt
                                else b.bill_edt
                            end,
                            case
                                when a.mth_sdt > b.bill_sdt then a.mth_sdt
                                else b.bill_sdt
                            end
                        ) + 1 as in_mth_days
                    from
                         {{ ref('AccrualHorizonCore') }} as a
                    inner join
                        {{ ref('RetailInvoiceCORE') }} as b
                        on b.bill_sdt <= a.mth_edt and b.bill_edt >= a.mth_sdt
                    where b.measure_name is not null
                ) as a
            where in_mth_days is not null
        ) as b
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
