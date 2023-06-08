/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



{{ config(materialized='table',alias='retailinvoicecore',post_hook='analyze table {{ this }} compute statistics') }}

with source_data as (
-- Combining RetailInvoiceVol,RetailInvoiceRevenue and AdjustmentDiscounts
    select
        -- Creating record_hash on the seasonality profile data group
        *,
        'MM' as market_segment,
        md5(
            concat(
                rmp_mth_no,
                market_sub_region,
                fuel_type,
                market_region,
                customer_segment
            )
        ) as record_hash,
        case
            when coalesce(ex_charge_desc_orion, '') = '' then trans_description
            when
                ex_charge_desc_orion like '%Supply%'
                then replace(trans_description, 'Supply', 'Daily')
            else ex_charge_desc_orion
        end as ex_charge_desc,
        -- Obtaining latest invoice of the customer
        dense_rank()
            over (partition by seq_product_item_id order by bill_sdt desc)
            as bill_sdt_rnk,
        dense_rank()
            over (partition by seq_product_item_id order by bill_edt desc)
            as bill_edt_rnk
    from
        (
            select distinct
                fuel_type,
                market_region,
                market_sub_region,
                customer_segment,
                seq_product_item_id,
                service_sdt,
                service_edt,
                bill_sdt,
                bill_edt,
                trans_description,
                trim(replace(replace(replace(case
                    when
                        trans_description not like 'Controlled%'
                        and charindex('(', trans_description, 1) > 1
                        then
                            trim(
                                left(
                                    trans_description,
                                    charindex('(', trans_description, 1) - 1
                                )
                            )
                    when
                        trans_description not like 'Controlled%'
                        and charindex('-', trans_description, 1) > 1
                        then
                            trim(
                                left(
                                    trans_description,
                                    charindex('-', trans_description, 1) - 1
                                )
                            )
                    else trans_description
                end, 'Off Peak', ''), 'Peak', ''), 'Rebate', ''))
                    as ex_charge_desc_orion,
                cntrl_load,
                solar_zr,
                measure_name,
                measure_code,
                measure_unit,
                NULL as daily_amt,
                daily_unit_quantity,
                daily_unit_quantity_zr,
                NULL as daily_disc_amt,
                NULL as daily_ppd_amt,
                NULL as daily_elig_ppd_amt,
                rmp_mth_no

            from {{ ref('RetailInvoiceVol') }}

            union all

            select distinct
                fuel_type,
                market_region,
                market_sub_region,
                customer_segment,
                seq_product_item_id,
                service_sdt,
                service_edt,
                bill_sdt,
                bill_edt,
                trans_description,
                trim(replace(replace(replace(case
                    when
                        trans_description not like 'Controlled%'
                        and charindex('(', trans_description, 1) > 1
                        then
                            trim(
                                left(
                                    trans_description,
                                    charindex('(', trans_description, 1) - 1
                                )
                            )
                    when
                        trans_description not like 'Controlled%'
                        and charindex('-', trans_description, 1) > 1
                        then
                            trim(
                                left(
                                    trans_description,
                                    charindex('-', trans_description, 1) - 1
                                )
                            )
                    else trans_description
                end, 'Off Peak', ''), 'Peak', ''), 'Rebate', ''))
                    as ex_charge_desc_orion,
                cntrl_load,
                solar_zr,
                measure_name,
                measure_code,
                measure_unit,
                daily_amt,
                NULL as daily_unit_quantity,
                NULL as daily_unit_quantity_zr,
                NULL as daily_disc_amt,
                NULL as daily_ppd_amt,
                NULL as daily_elig_ppd_amt,
                rmp_mth_no
            from {{ ref('RetailInvoiceRevenue') }}

            union all
            select distinct
                fuel_type,
                market_region,
                market_sub_region,
                customer_segment,
                seq_product_item_id,
                service_sdt,
                service_edt,
                bill_sdt,
                bill_edt,
                trans_description,
                trim(replace(replace(replace(case
                    when
                        trans_description not like 'Controlled%'
                        and charindex('(', trans_description, 1) > 1
                        then
                            trim(
                                left(
                                    trans_description,
                                    charindex('(', trans_description, 1) - 1
                                )
                            )
                    when
                        trans_description not like 'Controlled%'
                        and charindex('-', trans_description, 1) > 1
                        then
                            trim(
                                left(
                                    trans_description,
                                    charindex('-', trans_description, 1) - 1
                                )
                            )
                    else trans_description
                end, 'Off Peak', ''), 'Peak', ''), 'Rebate', ''))
                    as ex_charge_desc_orion,
                cntrl_load,
                solar_zr,
                measure_name,
                measure_code,
                measure_unit,
                NULL as daily_amt,
                NULL as daily_unit_quantity,
                NULL as daily_unit_quantity_zr,
                daily_disc_amt,
                daily_ppd_amt,
                daily_elig_ppd_amt,
                rmp_mth_no

            from {{ ref('AdjustmentDiscount') }}
        ) as t
    where measure_name is not null
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
