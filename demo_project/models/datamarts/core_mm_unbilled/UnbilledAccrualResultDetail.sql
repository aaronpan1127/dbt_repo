/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental') }}


WITH source_data AS (

    SELECT
        {{ var('UnbilledAccrualRunRequestID',25) }} AS unbilled_accrual_run_audit_id,
        '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}' AS unbilled_accrual_period_end_date,
        data_source,
        CASE
            WHEN fuel_type = 'GAS' THEN 'Gas'
            WHEN fuel_type = 'ELECTRICITY' THEN 'Elec'
        END AS fuel_type,
        record_type AS record_type,
        market_region AS market_region,
        market_sub_region AS market_sub_region,
        market_segment AS market_segment,
        customer_segment AS customer_segment,
        mth_sdt AS month_start_date,
        measure_name,
        measure_code,
        CASE
            WHEN measure_code LIKE 'VOL_DEMAND%' THEN 'MVA' ELSE measure_unit
        END AS measure_unit,
        sum(coalesce(measure, 0)) AS measure_value,
        seq_product_item_id AS service_id,
        cast(null AS string) AS meter_id,
        cast(null AS string) AS postal_code
    FROM {{ ref('CalcActualsDetailCOREmeter') }}
    GROUP BY
        data_source,
        fuel_type,
        record_type,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        mth_sdt,
        measure_name,
        measure_code,
        CASE
            WHEN measure_code LIKE 'VOL_DEMAND%' THEN 'MVA' ELSE measure_unit
        END,
        seq_product_item_id

    UNION ALL

    SELECT
        {{ var('UnbilledAccrualRunRequestID',25) }} AS unbilled_accrual_run_audit_id,
        '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}' AS unbilled_accrual_period_end_date,
        data_source,
        CASE
            WHEN fuel_type = 'GAS' THEN 'Gas'
            WHEN fuel_type = 'ELECTRICITY' THEN 'Elec'
        END AS fuel_type,
        record_type AS record_type,
        market_region AS market_region,
        market_sub_region AS market_sub_region,
        market_segment AS market_segment,
        customer_segment AS customer_segment,
        mth_sdt AS month_start_date,
        measure_name,
        measure_code,
        CASE
            WHEN measure_code LIKE 'VOL_DEMAND%' THEN 'MVA' ELSE measure_unit
        END AS measure_unit,
        sum(coalesce(measure, 0)) AS measure_value,
        seq_product_item_id AS service_id,
        cast(null AS string) AS meter_id,
        cast(null AS string) AS postal_code
    FROM {{ ref('CalcAccrualsExistingDetailCORE') }}
    GROUP BY
        data_source,
        fuel_type,
        record_type,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        mth_sdt,
        measure_name,
        measure_code,
        CASE
            WHEN measure_code LIKE 'VOL_DEMAND%' THEN 'MVA' ELSE measure_unit
        END,
        seq_product_item_id

    UNION ALL

    SELECT
        {{ var('UnbilledAccrualRunRequestID',25) }} AS unbilled_accrual_run_audit_id,
        '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}' AS unbilled_accrual_period_end_date,
        data_source,
        CASE
            WHEN fuel_type = 'GAS' THEN 'Gas'
            WHEN fuel_type = 'ELECTRICITY' THEN 'Elec'
        END AS fuel_type,
        record_type AS record_type,
        market_region AS market_region,
        market_sub_region AS market_sub_region,
        market_segment AS market_segment,
        customer_segment AS customer_segment,
        mth_sdt AS month_start_date,
        measure_name,
        measure_code,
        CASE
            WHEN measure_code LIKE 'VOL_DEMAND%' THEN 'MVA' ELSE measure_unit
        END AS measure_unit,
        sum(coalesce(measure, 0)) AS measure_value,
        seq_product_item_id AS service_id,
        cast(null AS string) AS meter_id,
        cast(null AS string) AS postal_code
    FROM {{ ref('CalcAccrualsNeverBilledDetailCORE') }}
    GROUP BY
        data_source,
        fuel_type,
        record_type,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        mth_sdt,
        measure_name,
        measure_code,
        CASE
            WHEN measure_code LIKE 'VOL_DEMAND%' THEN 'MVA' ELSE measure_unit
        END,
        seq_product_item_id

)

SELECT
    '{{ invocation_id }}' AS run_id,
    *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
