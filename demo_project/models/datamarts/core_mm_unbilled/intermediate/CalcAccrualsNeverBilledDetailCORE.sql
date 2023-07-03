/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='calcaccrualsneverbilleddetailcore') }}

WITH source_data AS (
    SELECT DISTINCT
        'Core' AS data_source,
        'Accrual (Never Billed)' AS record_type,
        seq_product_item_id,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        fuel_type,
        trans_description,
        cntrl_load,
        solar_zr,
        'NULL' AS bill_sdt,
        'NULL' AS bill_edt,
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
    FROM (
        SELECT DISTINCT
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
            a.in_mth_days AS total_days,
            a.avg_daily_amt AS daily_amt,
            a.avg_daily_unit_quantity AS daily_unit_quantity,
            a.avg_daily_unit_quantity_zr AS daily_unit_quantity_zr,
            a.avg_daily_disc_amt AS daily_disc_amt,
            a.avg_daily_ppd_amt AS daily_ppd_amt,
            a.avg_daily_elig_ppd_amt AS daily_elig_ppd_amt,
            (a.account_number_count / a.record_count) AS factor,
            -- Mutiplying by a factor to avg_daily values to rationalize the measures values for Never billed account, 
            -- since the components in the bill are unknown for a never billed account.
            -- Factor  = 
            -- (No of distinct accounts in the seasonality profile data group,trans_description,
            -- measure_name,cntrl_load,solar_zr)/
            -- (Total accounts in the seasonality profile data group,trans_description,
            -- measure_name,cntrl_load,solar_zr)
            CASE
                WHEN a.solar_zr = 'Y' THEN a.avg_daily_unit_quantity_zr
                WHEN a.record_count = 0 THEN 0
                ELSE
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_unit_quantity
                    )
            END
            * a.in_mth_days AS mth_qty,
            CASE
                WHEN a.solar_zr = 'Y' THEN 0
                WHEN a.record_count = 0 THEN 0
                ELSE
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_amt
                    )
            END
            * a.in_mth_days AS amount,
            CASE
                WHEN a.solar_zr = 'Y' THEN 0
                WHEN a.record_count = 0 THEN 0
                ELSE
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_disc_amt
                    )
            END
            * a.in_mth_days AS discount,
            CASE
                WHEN a.solar_zr = 'Y' THEN 0
                WHEN a.record_count = 0 THEN 0
                ELSE
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_ppd_amt
                    )
            END
            * a.in_mth_days AS ppd_amount,
            CASE
                WHEN a.solar_zr = 'Y' THEN 0
                WHEN a.record_count = 0 THEN 0
                ELSE
                    (
                        (a.account_number_count / a.record_count)
                        * a.avg_daily_elig_ppd_amt
                    )
            END
            * a.in_mth_days AS elig_ppd_amount
        FROM
            (
                SELECT DISTINCT
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
                    datediff(
                        -- Including last day in in_mth_days
                        CASE
                            WHEN m.mth_edt < m.service_edt THEN m.mth_edt
                            ELSE m.service_edt
                        END,
                        CASE
                            WHEN m.mth_sdt > m.service_sdt THEN m.mth_sdt
                            ELSE m.service_sdt
                        END
                    ) + 1 AS in_mth_days
                FROM
                    (
                        SELECT DISTINCT
                            record_hash,
                            seq_product_item_id,
                            mth_sdt,
                            mth_edt,
                            service_sdt,
                            service_edt
                        FROM {{ ref('NeverBilledCORE') }}
                    ) AS m
                INNER JOIN
                    (
                        SELECT * FROM {{ ref('SeasonalAvgNBCORE') }}
                        WHERE trans_description NOT LIKE '%Import%'
                    ) AS s
                    ON m.record_hash = s.record_hash
            ) AS a
        WHERE
            a.in_mth_days IS NOT NULL AND a.in_mth_days > 0
    ) AS t
)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
