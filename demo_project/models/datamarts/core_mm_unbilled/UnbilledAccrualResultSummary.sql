/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental') }}


WITH unbilledaccrualresultsummary_tmp AS (

    SELECT
        unbilled_accrual_run_audit_id,
        unbilled_accrual_period_end_date,
        data_source,
        fuel_type,
        record_type,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        month_start_date,
        measure_name,
        measure_code,
        measure_unit,
        sum(measure_value) AS measure_value,
        count(service_id) AS service_count
    FROM {{ ref('UnbilledAccrualResultDetail') }} WHERE
        data_source = 'Core'
        AND unbilled_accrual_run_audit_id = {{ var('UnbilledAccrualRunRequestID',25) }}
    GROUP BY
        unbilled_accrual_run_audit_id,
        unbilled_accrual_period_end_date,
        data_source,
        fuel_type,
        record_type,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        month_start_date,
        measure_name,
        measure_code,
        measure_unit
),

source_data AS (
    SELECT * FROM unbilledaccrualresultsummary_tmp
    UNION ALL

    SELECT
        {{ var('UnbilledAccrualRunRequestID',25) }} AS unbilled_accrual_run_audit_id,
        '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}' AS unbilled_accrual_period_end_date,
        data_source,
        fuel_type,
        record_type,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        month_start_date,
        'Service Count' AS measure_name,
        'SERVICES' AS measure_code,
        'Meters' AS measure_unit,
        service_count AS measure_value,
        service_count AS service_count
    FROM unbilledaccrualresultsummary_tmp
    WHERE
        data_source = 'Core' AND measure_code LIKE 'DAY%'

)

SELECT
    '{{ invocation_id }}' AS run_id,
    *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
