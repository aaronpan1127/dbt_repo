/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='calcaccrualsexistingdetailcore') }}



WITH source_data AS (
    SELECT DISTINCT
        'Core' AS data_source,
        'Accrual (Existing)' AS record_type,
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
            SELECT DISTINCT
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
                a.deemed_avg_qty * a.in_mth_days AS mth_qty,
                a.daily_amt * a.in_mth_days AS amount,
                a.daily_disc_amt * a.in_mth_days AS discount,
                a.daily_ppd_amt * a.in_mth_days AS ppd_amount,
                a.daily_elig_ppd_amt * a.in_mth_days AS elig_ppd_amount
            FROM
                (
                    SELECT DISTINCT
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
                        CASE
                            WHEN
                                sa.avg_daily_unit_quantity IS NULL
                                THEN 1
                            ELSE
                                sa.avg_daily_unit_quantity
                                / s.avg_daily_unit_quantity
                        END
                        * CASE WHEN s.solar_zr = 'Y' THEN s.daily_unit_quantity_zr ELSE s.daily_unit_quantity
                        END AS deemed_avg_qty,
                        CASE WHEN s.solar_zr = 'Y' THEN 0 ELSE s.daily_amt END
                        AS daily_amt,
                        CASE
                            WHEN s.solar_zr = 'Y' THEN 0 ELSE s.daily_disc_amt
                        END AS daily_disc_amt,
                        CASE
                            WHEN s.solar_zr = 'Y' THEN 0 ELSE s.daily_ppd_amt
                        END AS daily_ppd_amt,
                        CASE
                            WHEN s.solar_zr = 'Y' THEN 0 ELSE
                                s.daily_elig_ppd_amt
                        END AS daily_elig_ppd_amt,
                        datediff(
                            -- Including last day in in_mth_days
                            date_add(CASE
                                WHEN
                                    (
                                        s.service_edt >= a.mth_sdt
                                        OR s.service_edt IS NULL
                                    )
                                    AND (s.service_sdt <= a.mth_edt)
                                    THEN
                                        CASE
                                            -- 3A
                                            WHEN
                                                s.bill_edt < a.mth_sdt
                                                AND (
                                                    a.mth_edt <= s.service_edt
                                                    OR s.service_edt IS NULL
                                                )
                                                THEN a.mth_edt
                                            -- 3B
                                            WHEN
                                                s.bill_edt >= a.mth_sdt
                                                AND (
                                                    a.mth_edt <= s.service_edt
                                                    OR s.service_edt IS NULL
                                                )
                                                THEN a.mth_edt
                                            --2Bi
                                            WHEN
                                                s.bill_edt >= a.mth_sdt
                                                AND a.mth_edt >= s.service_edt
                                                THEN s.service_edt
                                            -- 2A
                                            WHEN
                                                s.bill_edt < a.mth_sdt
                                                AND (
                                                    a.mth_edt >= s.service_edt
                                                    OR s.service_edt IS NULL
                                                )
                                                THEN s.service_edt
                                        END
                            END, 1),
                            CASE
                                WHEN
                                    (
                                        s.service_edt >= a.mth_sdt
                                        OR s.service_edt IS NULL
                                    )
                                    AND (s.service_sdt <= a.mth_edt)
                                    THEN
                                        CASE
                                            -- 3A
                                            WHEN
                                                s.bill_edt < a.mth_sdt
                                                AND (
                                                    a.mth_edt <= s.service_edt
                                                    OR s.service_edt IS NULL
                                                )
                                                THEN a.mth_sdt
                                            --3B
                                            WHEN
                                                s.bill_edt >= a.mth_sdt
                                                AND (
                                                    a.mth_edt <= s.service_edt
                                                    OR s.service_edt IS NULL
                                                )
                                                THEN date_add(s.bill_edt, 1)
                                            --2Bi
                                            WHEN
                                                s.bill_edt >= a.mth_sdt
                                                AND a.mth_edt >= s.service_edt
                                                THEN date_add(s.bill_edt, 1)
                                            -- 2A
                                            WHEN
                                                s.bill_edt < a.mth_sdt
                                                AND (
                                                    a.mth_edt >= s.service_edt
                                                    OR s.service_edt IS NULL
                                                )
                                                THEN a.mth_sdt
                                        END
                            END
                        )
                        AS in_mth_days
                    FROM
                        {{ ref('AccrualHorizonCore') }} AS a
                    INNER JOIN
                        (
                            SELECT DISTINCT
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
                                rc.ex_charge_desc AS trans_description,
                                rc.daily_amt,
                                rc.daily_unit_quantity,
                                rc.daily_unit_quantity_zr,
                                rc.daily_disc_amt,
                                rc.daily_ppd_amt,
                                rc.daily_elig_ppd_amt,
                                coalesce(
                                    sa.avg_daily_unit_quantity,
                                    rc.daily_unit_quantity
                                )
                                AS avg_daily_unit_quantity
                            FROM
                                (
                                    SELECT * FROM {{ ref('RetailInvoiceCORE') }}
                                    WHERE
                                        (bill_sdt_rnk = 1 OR bill_edt_rnk = 1)
                                        AND bill_edt
                                        <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
                                ) AS rc
                            LEFT JOIN
                                {{ ref('SeasonalAvgNBCORE') }} AS sa
                                ON
                                    rc.record_hash = sa.record_hash
                                    AND rc.solar_zr = sa.solar_zr
                                    AND rc.cntrl_load = sa.cntrl_load
                                    AND rc.ex_charge_desc = sa.trans_description
                                    AND rc.measure_name = sa.measure_name
                                    AND rc.measure_code = sa.measure_code
                        ) AS s
                        ON
                            (
                                s.service_sdt <= a.mth_edt
                                AND s.service_edt >= a.mth_sdt
                            )
                            AND s.bill_edt <= a.mth_edt
                    LEFT JOIN
                        {{ ref('SeasonalAvgNBCORE') }} AS sa
                        ON
                            sa.mth_no = a.mth_no
                            AND sa.market_sub_region = s.market_sub_region
                            AND sa.fuel_type = s.fuel_type
                            AND sa.market_region = s.market_region
                            AND sa.customer_segment = s.customer_segment
                            AND sa.solar_zr = s.solar_zr
                            AND sa.cntrl_load = s.cntrl_load
                            AND sa.trans_description = s.trans_description
                            AND sa.measure_name = s.measure_name
                            AND sa.measure_code = s.measure_code
                ) AS a
            WHERE in_mth_days IS NOT NULL AND in_mth_days > 0
        ) AS b
)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
