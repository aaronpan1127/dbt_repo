/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental') }}


with unbilledaccrualresultsummary_tmp as (

    select
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
        SUM(measure_value) as measure_value,
        COUNT(service_id) as service_count
    from {{ ref('UnbilledAccrualResultDetail') }} where
        data_source = 'Core'
        and unbilled_accrual_run_audit_id = {{ var('UnbilledAccrualRunRequestID',25) }}
    group by
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
source_data as (
    select * from unbilledaccrualresultsummary_tmp
    union all

    select
        {{ var('UnbilledAccrualRunRequestID',25) }} as unbilled_accrual_run_audit_id,
        '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}' as unbilled_accrual_period_end_date,
        data_source,
        fuel_type,
        record_type,
        market_region,
        market_sub_region,
        market_segment,
        customer_segment,
        month_start_date,
        'Service Count' as measure_name,
        'SERVICES' as measure_code,
        'Meters' as measure_unit,
        service_count as measure_value,
        service_count as service_count
    from unbilledaccrualresultsummary_tmp
    where
        data_source = 'Core' and measure_code like 'DAY%'

)

select '{{invocation_id}}' as run_id,*
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
