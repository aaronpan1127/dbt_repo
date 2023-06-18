/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/



{{ config(materialized='table',alias='seasonalavgnbcore',post_hook='analyze table {{ this }} compute statistics') }}

WITH t AS (
    SELECT
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
        ) AS record_hash,
        date_part('MONTH', s.mth_sdt) AS mth_no,
        count(DISTINCT seq_product_item_id) AS record_count
    FROM
        {{ ref('RetailInvoiceCORE') }} AS b
    CROSS JOIN
        {{ ref('SeasonalityHorizonCore') }} AS s
    WHERE
        b.bill_sdt <= s.mth_edt
        AND b.bill_edt >= s.mth_sdt
        AND measure_name IS NOT NULL
    GROUP BY
        b.fuel_type,
        b.market_region,
        b.market_segment,
        b.customer_segment,
        b.market_sub_region,
        date_part('MONTH', s.mth_sdt)

),

source_data AS (
    SELECT
        a.*,
        t.record_count
    FROM
        (
            SELECT DISTINCT
                b.fuel_type,
                b.market_region,
                b.market_segment,
                b.customer_segment,
                b.market_sub_region,
                b.cntrl_load,
                b.solar_zr,
                b.ex_charge_desc AS trans_description,
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
                ) AS record_hash,
                date_part('MONTH', s.mth_sdt) AS mth_no,
                round(avg(b.daily_unit_quantity), 5) AS avg_daily_unit_quantity,
                round(avg(b.daily_unit_quantity_zr), 5)
                AS avg_daily_unit_quantity_zr,
                round(avg(b.daily_amt), 5) AS avg_daily_amt,
                round(avg(b.daily_disc_amt), 5) AS avg_daily_disc_amt,
                round(avg(b.daily_ppd_amt), 5) AS avg_daily_ppd_amt,
                round(avg(b.daily_elig_ppd_amt), 5) AS avg_daily_elig_ppd_amt,
                count(DISTINCT b.seq_product_item_id) AS account_number_count
            FROM
                {{ ref('RetailInvoiceCORE') }} AS b
            CROSS JOIN
                {{ ref('SeasonalityHorizonCore') }} AS s
            WHERE
                b.bill_sdt <= s.mth_edt
                AND b.bill_edt >= s.mth_sdt
                AND measure_name IS NOT NULL
            GROUP BY
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

        ) AS a
    INNER JOIN

        t
        ON a.record_hash = t.record_hash
)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
