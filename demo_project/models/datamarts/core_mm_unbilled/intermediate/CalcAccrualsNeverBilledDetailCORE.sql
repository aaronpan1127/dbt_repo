/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='calcaccrualsneverbilleddetailcore') }}

with source_data as (
    select distinct
        'Core' as data_source,
        'Accrual (Never Billed)' as record_type,
        seq_product_item_id,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        fuel_type,
        trans_description,
        cntrl_load,
        solar_zr,
        'NULL' as bill_sdt,
        'NULL' as bill_edt,
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
                then cast(mth_qty as NUMERIC(28, 15)) / 1000
            when measure_code like 'DIS%' then cast(discount as NUMERIC(28, 15))
            when
                measure_code like 'PPD%'
                then cast(ppd_amount as NUMERIC(28, 15))
            when
                measure_code like 'EPPD%'
                then cast(elig_ppd_amount as NUMERIC(28, 15))
            when measure_code like 'REV%' then cast(amount as NUMERIC(28, 15))
        end as measure
    from (
        select distinct
            a.seq_product_item_id,
            a.market_region,
            a.market_sub_region,
            a.market_segment,
            a.customer_segment,
            a.fuel_type,
            a.cntrl_load,
            a.trans_description,
            a.solar_zr,
            a.mth_sdt,
            a.mth_edt,
            a.service_sdt,
            a.service_edt,
            a.measure_name,
            a.measure_code,
            a.measure_unit,
            a.account_number_count,
            a.record_count,
            a.in_mth_days as total_days,
            a.avg_daily_amt as daily_amt,
            a.avg_daily_unit_quantity as daily_unit_quantity,
            a.avg_daily_unit_quantity_zr as daily_unit_quantity_zr,
            a.avg_daily_disc_amt as daily_disc_amt,
            a.avg_daily_ppd_amt as daily_ppd_amt,
            a.avg_daily_elig_ppd_amt as daily_elig_ppd_amt,
            (a.account_number_count / a.record_count) as factor,
            -- Mutiplying by a factor to avg_daily values to rationalize the measures values for Never billed account, since the components in the bill are unknown for a never billed account.
            -- Factor  = (No of distinct accounts in the seasonality profile data group,trans_description,measure_name,cntrl_load,solar_zr)/
            --           (Total accounts in the seasonality profile data group,trans_description,measure_name,cntrl_load,solar_zr)
            case
                when a.solar_zr = 'Y' then a.avg_daily_unit_quantity_zr
                when a.record_count = 0 then 0
                else
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_unit_quantity
                    )
            end
            * a.in_mth_days as mth_qty,
            case
                when a.solar_zr = 'Y' then 0
                when a.record_count = 0 then 0
                else
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_amt
                    )
            end
            * a.in_mth_days as amount,
            case
                when a.solar_zr = 'Y' then 0
                when a.record_count = 0 then 0
                else
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_disc_amt
                    )
            end
            * a.in_mth_days as discount,
            case
                when a.solar_zr = 'Y' then 0
                when a.record_count = 0 then 0
                else
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_ppd_amt
                    )
            end
            * a.in_mth_days as ppd_amount,
            case
                when a.solar_zr = 'Y' then 0
                when a.record_count = 0 then 0
                else
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_elig_ppd_amt
                    )
            end
            * a.in_mth_days as elig_ppd_amount
        from
            (
                select distinct
                    m.seq_product_item_id,
                    s.market_region,
                    s.market_sub_region,
                    s.market_segment,
                    s.customer_segment,
                    s.fuel_type,
                    s.trans_description,
                    s.cntrl_load,
                    s.solar_zr,
                    m.mth_sdt,
                    m.mth_edt,
                    m.service_sdt,
                    m.service_edt,
                    s.measure_name,
                    s.measure_code,
                    s.measure_unit,
                    s.avg_daily_amt,
                    s.avg_daily_unit_quantity,
                    s.avg_daily_unit_quantity_zr,
                    s.avg_daily_disc_amt,
                    s.avg_daily_ppd_amt,
                    s.avg_daily_elig_ppd_amt,
                    s.account_number_count,
                    s.record_count,
                    DATEDIFF(
                        -- Including last day in in_mth_days
                        case
                            when m.mth_edt < m.service_edt then m.mth_edt
                            else m.service_edt
                        end,
                        case
                            when m.mth_sdt > m.service_sdt then m.mth_sdt
                            else m.service_sdt
                        end
                    ) + 1 as in_mth_days
                from
                    (
                        select distinct
                            record_hash,
                            seq_product_item_id,
                            mth_sdt,
                            mth_edt,
                            service_sdt,
                            service_edt
                        from {{ ref('NeverBilledCORE') }}
                    ) as m
                inner join
                    (
                        select * from {{ ref('SeasonalAvgNBCORE') }}
                        where trans_description not like '%Import%'
                    ) as s
                    on m.record_hash = s.record_hash
            ) as a
        where
            in_mth_days is not null and in_mth_days > 0
    ) as t
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
