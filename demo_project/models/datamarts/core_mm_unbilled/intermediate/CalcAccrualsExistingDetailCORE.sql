/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='calcaccrualsexistingdetailcore') }}



with source_data as (
    select distinct
        'Core' as data_source,
        'Accrual (Existing)' as record_type,
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
            select distinct
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
                a.deemed_avg_qty * a.in_mth_days as mth_qty,
                a.daily_amt * a.in_mth_days as amount,
                a.daily_disc_amt * a.in_mth_days as discount,
                a.daily_ppd_amt * a.in_mth_days as ppd_amount,
                a.daily_elig_ppd_amt * a.in_mth_days as elig_ppd_amount
            from
                (
                    select distinct
                        s.seq_product_item_id,
                        s.market_region,
                        s.market_sub_region,
                        s.market_segment,
                        s.customer_segment,
                        s.fuel_type,
                        s.trans_description,
                        s.cntrl_load,
                        s.solar_zr,
                        s.bill_sdt,
                        s.bill_edt,
                        a.mth_sdt,
                        a.mth_edt,
                        s.service_sdt,
                        s.service_edt,
                        s.measure_name,
                        s.measure_code,
                        s.measure_unit,
                        s.daily_unit_quantity,
                        s.daily_unit_quantity_zr,
                        case
                            when
                                sa.avg_daily_unit_quantity is null
                                then 1
                            else
                                sa.avg_daily_unit_quantity
                                / s.avg_daily_unit_quantity
                        end
                        * case when s.solar_zr = 'Y' then s.daily_unit_quantity_zr else s.daily_unit_quantity
                        end as deemed_avg_qty,
                        case when s.solar_zr = 'Y' then 0 else s.daily_amt end
                            as daily_amt,
                        case
                            when s.solar_zr = 'Y' then 0 else s.daily_disc_amt
                        end as daily_disc_amt,
                        case
                            when s.solar_zr = 'Y' then 0 else s.daily_ppd_amt
                        end as daily_ppd_amt,
                        case
                            when s.solar_zr = 'Y' then 0 else
                                s.daily_elig_ppd_amt
                        end as daily_elig_ppd_amt,
                        DATEDIFF(
                            -- Including last day in in_mth_days
                            DATE_ADD(case
                                when
                                    (
                                        s.service_edt >= a.mth_sdt
                                        or s.service_edt is null
                                    )
                                    and (s.service_sdt <= a.mth_edt)
                                    then
                                        case
                                            -- 3A
                                            when
                                                s.bill_edt < a.mth_sdt
                                                and (
                                                    a.mth_edt <= s.service_edt
                                                    or s.service_edt is null
                                                )
                                                then a.mth_edt
                                            -- 3B
                                            when
                                                s.bill_edt >= a.mth_sdt
                                                and (
                                                    a.mth_edt <= s.service_edt
                                                    or s.service_edt is null
                                                )
                                                then a.mth_edt
                                            --2Bi
                                            when
                                                s.bill_edt >= a.mth_sdt
                                                and a.mth_edt >= s.service_edt
                                                then s.service_edt
                                            -- 2A
                                            when
                                                s.bill_edt < a.mth_sdt
                                                and (
                                                    a.mth_edt >= s.service_edt
                                                    or s.service_edt is null
                                                )
                                                then s.service_edt
                                        end
                            end, 1),
                            case
                                when
                                    (
                                        s.service_edt >= a.mth_sdt
                                        or s.service_edt is null
                                    )
                                    and (s.service_sdt <= a.mth_edt)
                                    then
                                        case
                                            -- 3A
                                            when
                                                s.bill_edt < a.mth_sdt
                                                and (
                                                    a.mth_edt <= s.service_edt
                                                    or s.service_edt is null
                                                )
                                                then a.mth_sdt
                                            --3B
                                            when
                                                s.bill_edt >= a.mth_sdt
                                                and (
                                                    a.mth_edt <= s.service_edt
                                                    or s.service_edt is null
                                                )
                                                then DATE_ADD(s.bill_edt, 1)
                                            --2Bi
                                            when
                                                s.bill_edt >= a.mth_sdt
                                                and a.mth_edt >= s.service_edt
                                                then DATE_ADD(s.bill_edt, 1)
                                            -- 2A
                                            when
                                                s.bill_edt < a.mth_sdt
                                                and (
                                                    a.mth_edt >= s.service_edt
                                                    or s.service_edt is null
                                                )
                                                then a.mth_sdt
                                        end
                            end
                        )
                            as in_mth_days
                    from
                        {{ ref('AccrualHorizonCore') }} as a
                    inner join
                        (
                            select distinct
                                rc.seq_product_item_id,
                                rc.market_region,
                                rc.market_sub_region,
                                rc.market_segment,
                                rc.customer_segment,
                                rc.fuel_type,
                                rc.cntrl_load,
                                rc.solar_zr,
                                rc.bill_sdt,
                                rc.bill_edt,
                                rc.service_sdt,
                                rc.service_edt,
                                rc.measure_name,
                                rc.measure_code,
                                rc.measure_unit,
                                rc.ex_charge_desc as trans_description,
                                rc.daily_amt,
                                rc.daily_unit_quantity,
                                rc.daily_unit_quantity_zr,
                                rc.daily_disc_amt,
                                rc.daily_ppd_amt,
                                rc.daily_elig_ppd_amt,
                                coalesce (sa.avg_daily_unit_quantity,
                                rc.daily_unit_quantity)
                                    as avg_daily_unit_quantity
                            from
                                (
                                    select * from {{ ref('RetailInvoiceCORE') }}
                                    where
                                        (bill_sdt_rnk = 1 or bill_edt_rnk = 1)
                                        and bill_edt
                                        <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
                                ) as rc
                            left join
                                {{ ref('SeasonalAvgNBCORE') }} as sa
                                on
                                    rc.record_hash = sa.record_hash
                                    and rc.solar_zr = sa.solar_zr
                                    and rc.cntrl_load = sa.cntrl_load
                                    and rc.ex_charge_desc = sa.trans_description
                                    and rc.measure_name = sa.measure_name
                                    and rc.measure_code = sa.measure_code
                        ) as s
                        on
                            (
                                s.service_sdt <= a.mth_edt
                                and s.service_edt >= a.mth_sdt
                            )
                            and s.bill_edt <= a.mth_edt
                    left join
                        {{ ref('SeasonalAvgNBCORE') }} as sa
                        on
                            sa.mth_no = a.mth_no
                            and sa.market_sub_region = s.market_sub_region
                            and sa.fuel_type = s.fuel_type
                            and sa.market_region = s.market_region
                            and sa.customer_segment = s.customer_segment
                            and sa.solar_zr = s.solar_zr
                            and sa.cntrl_load = s.cntrl_load
                            and sa.trans_description = s.trans_description
                            and sa.measure_name = s.measure_name
                            and sa.measure_code = s.measure_code
                ) as a
            where in_mth_days is not null and in_mth_days > 0
        ) as b
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
