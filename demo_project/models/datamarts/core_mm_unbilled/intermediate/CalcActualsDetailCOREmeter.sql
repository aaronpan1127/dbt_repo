/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='calcactualsdetailcoremeter') }}


WITH source_data AS (
    SELECT DISTINCT
        'Core' AS data_source,
        'Actual' AS record_type,
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
        CASE
            WHEN measure_code = 'DAY_SUPPLY' THEN total_days
            WHEN
                measure_code LIKE 'VOL%'
                THEN cast(mth_qty AS numeric(28, 15)) / 1000
            WHEN measure_code LIKE 'DIS%' THEN cast(discount AS numeric(28, 15))
            WHEN
                measure_code LIKE 'PPD%'
                THEN cast(ppd_amount AS numeric(28, 15))
            WHEN
                measure_code LIKE 'EPPD%'
                THEN cast(elig_ppd_amount AS numeric(28, 15))
            WHEN measure_code LIKE 'REV%' THEN cast(amount AS numeric(28, 15))
        END AS measure
    FROM
        (
            SELECT
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
                a.in_mth_days AS total_days,
                a.daily_amt,
                a.daily_unit_quantity,
                a.daily_unit_quantity_zr,
                a.daily_disc_amt,
                a.daily_ppd_amt,
                a.daily_elig_ppd_amt,
                CASE
                    WHEN a.solar_zr = 'Y' THEN a.daily_unit_quantity_zr ELSE
                        a.daily_unit_quantity
                END
                * a.in_mth_days AS mth_qty,
                CASE WHEN a.solar_zr = 'Y' THEN 0 ELSE a.daily_amt END
                * a.in_mth_days AS amount,
                CASE WHEN a.solar_zr = 'Y' THEN 0 ELSE a.daily_disc_amt END
                * a.in_mth_days AS discount,
                CASE WHEN a.solar_zr = 'Y' THEN 0 ELSE a.daily_ppd_amt END
                * a.in_mth_days AS ppd_amount,
                CASE WHEN a.solar_zr = 'Y' THEN 0 ELSE a.daily_elig_ppd_amt END
                * a.in_mth_days AS elig_ppd_amount
            FROM
                (
                    SELECT DISTINCT
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
                        datediff(
                            CASE
                                WHEN a.mth_edt < b.bill_edt THEN a.mth_edt
                                ELSE b.bill_edt
                            END,
                            CASE
                                WHEN a.mth_sdt > b.bill_sdt THEN a.mth_sdt
                                ELSE b.bill_sdt
                            END
                        ) + 1 AS in_mth_days
                    FROM
                        {{ ref('AccrualHorizonCore') }} AS a
                    INNER JOIN
                        {{ ref('RetailInvoiceCORE') }} AS b
                        ON b.bill_sdt <= a.mth_edt AND b.bill_edt >= a.mth_sdt
                    WHERE b.measure_name IS NOT NULL
                ) AS a
            WHERE in_mth_days IS NOT NULL
        ) AS b
)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
