/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



{{ config(materialized='table',alias='retailinvoicecore',post_hook='analyze table {{ this }} compute statistics') }}

WITH source_data AS (
-- Combining RetailInvoiceVol,RetailInvoiceRevenue and AdjustmentDiscounts
    SELECT
        -- Creating record_hash on the seasonality profile data group
        *,
        'MM' AS market_segment,
        md5(
            concat(
                rmp_mth_no,
                market_sub_region,
                fuel_type,
                market_region,
                customer_segment
            )
        ) AS record_hash,
        CASE
            WHEN coalesce(ex_charge_desc_orion, '') = '' THEN trans_description
            WHEN
                ex_charge_desc_orion LIKE '%Supply%'
                THEN replace(trans_description, 'Supply', 'Daily')
            ELSE ex_charge_desc_orion
        END AS ex_charge_desc,
        -- Obtaining latest invoice of the customer
        dense_rank()
            OVER (PARTITION BY seq_product_item_id ORDER BY bill_sdt DESC)
        AS bill_sdt_rnk,
        dense_rank()
            OVER (PARTITION BY seq_product_item_id ORDER BY bill_edt DESC)
        AS bill_edt_rnk
    FROM
        (
            SELECT DISTINCT
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
                trim(replace(replace(replace(CASE
                    WHEN
                        trans_description NOT LIKE 'Controlled%'
                        AND charindex('(', trans_description, 1) > 1
                        THEN
                            trim(
                                left(
                                    trans_description,
                                    charindex('(', trans_description, 1) - 1
                                )
                            )
                    WHEN
                        trans_description NOT LIKE 'Controlled%'
                        AND charindex('-', trans_description, 1) > 1
                        THEN
                            trim(
                                left(
                                    trans_description,
                                    charindex('-', trans_description, 1) - 1
                                )
                            )
                    ELSE trans_description
                END, 'Off Peak', ''), 'Peak', ''), 'Rebate', ''))
                AS ex_charge_desc_orion,
                cntrl_load,
                solar_zr,
                measure_name,
                measure_code,
                measure_unit,
                null AS daily_amt,
                daily_unit_quantity,
                daily_unit_quantity_zr,
                null AS daily_disc_amt,
                null AS daily_ppd_amt,
                null AS daily_elig_ppd_amt,
                rmp_mth_no

            FROM {{ ref('RetailInvoiceVol') }}

            UNION ALL

            SELECT DISTINCT
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
                trim(replace(replace(replace(CASE
                    WHEN
                        trans_description NOT LIKE 'Controlled%'
                        AND charindex('(', trans_description, 1) > 1
                        THEN
                            trim(
                                left(
                                    trans_description,
                                    charindex('(', trans_description, 1) - 1
                                )
                            )
                    WHEN
                        trans_description NOT LIKE 'Controlled%'
                        AND charindex('-', trans_description, 1) > 1
                        THEN
                            trim(
                                left(
                                    trans_description,
                                    charindex('-', trans_description, 1) - 1
                                )
                            )
                    ELSE trans_description
                END, 'Off Peak', ''), 'Peak', ''), 'Rebate', ''))
                AS ex_charge_desc_orion,
                cntrl_load,
                solar_zr,
                measure_name,
                measure_code,
                measure_unit,
                daily_amt,
                null AS daily_unit_quantity,
                null AS daily_unit_quantity_zr,
                null AS daily_disc_amt,
                null AS daily_ppd_amt,
                null AS daily_elig_ppd_amt,
                rmp_mth_no
            FROM {{ ref('RetailInvoiceRevenue') }}

            UNION ALL
            SELECT DISTINCT
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
                trim(replace(replace(replace(CASE
                    WHEN
                        trans_description NOT LIKE 'Controlled%'
                        AND charindex('(', trans_description, 1) > 1
                        THEN
                            trim(
                                left(
                                    trans_description,
                                    charindex('(', trans_description, 1) - 1
                                )
                            )
                    WHEN
                        trans_description NOT LIKE 'Controlled%'
                        AND charindex('-', trans_description, 1) > 1
                        THEN
                            trim(
                                left(
                                    trans_description,
                                    charindex('-', trans_description, 1) - 1
                                )
                            )
                    ELSE trans_description
                END, 'Off Peak', ''), 'Peak', ''), 'Rebate', ''))
                AS ex_charge_desc_orion,
                cntrl_load,
                solar_zr,
                measure_name,
                measure_code,
                measure_unit,
                null AS daily_amt,
                null AS daily_unit_quantity,
                null AS daily_unit_quantity_zr,
                daily_disc_amt,
                daily_ppd_amt,
                daily_elig_ppd_amt,
                rmp_mth_no

            FROM {{ ref('AdjustmentDiscount') }}
        ) AS t
    WHERE measure_name IS NOT NULL
)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
