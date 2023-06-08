/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental') }}


with source_data as (

    select
        {{ var('UnbilledAccrualRunRequestID',25) }} as unbilled_accrual_run_audit_id,
        '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}' as unbilled_accrual_period_end_date,
        data_source,
        case
            when fuel_type = 'GAS' then 'Gas'
            when fuel_type = 'ELECTRICITY' then 'Elec'
        end as fuel_type,        
        record_type as record_type,
        market_region as market_region,
        market_sub_region as market_sub_region,
        market_segment as market_segment,
        customer_segment as customer_segment,
        mth_sdt as month_start_date,
        measure_name,
        measure_code,
        case
            when measure_code like 'VOL_DEMAND%' then 'MVA' else measure_unit
        end as measure_unit,        
        SUM(COALESCE(measure, 0)) as measure_value,        
        seq_product_item_id as service_id,
        cast(NULL as string) as meter_id,
        cast(NULL as string) as postal_code
    from {{ ref('CalcActualsDetailCOREmeter') }}
    group by
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
        case
            when measure_code like 'VOL_DEMAND%' then 'MVA' else measure_unit
        end,
        seq_product_item_id

    union all

    select
        {{ var('UnbilledAccrualRunRequestID',25) }} as unbilled_accrual_run_audit_id,
        '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}' as unbilled_accrual_period_end_date,
        data_source,
        case
            when fuel_type = 'GAS' then 'Gas'
            when fuel_type = 'ELECTRICITY' then 'Elec'
        end as fuel_type,
        record_type as record_type,
        market_region as market_region,
        market_sub_region as market_sub_region,
        market_segment as market_segment,
        customer_segment as customer_segment,
        mth_sdt as month_start_date,
        measure_name,
        measure_code,
        case
            when measure_code like 'VOL_DEMAND%' then 'MVA' else measure_unit
        end as measure_unit,
        SUM(COALESCE(measure, 0)) as measure_value,
        seq_product_item_id as service_id,
        cast(NULL as string) as meter_id,
        cast(NULL as string) as postal_code
    from {{ ref('CalcAccrualsExistingDetailCORE') }}
    group by
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
        case
            when measure_code like 'VOL_DEMAND%' then 'MVA' else measure_unit
        end,
        seq_product_item_id

    union all

    select
        {{ var('UnbilledAccrualRunRequestID',25) }} as unbilled_accrual_run_audit_id,
        '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}' as unbilled_accrual_period_end_date,
        data_source,
        case
            when fuel_type = 'GAS' then 'Gas'
            when fuel_type = 'ELECTRICITY' then 'Elec'
        end as fuel_type,
        record_type as record_type,
        market_region as market_region,
        market_sub_region as market_sub_region,
        market_segment as market_segment,
        customer_segment as customer_segment,
        mth_sdt as month_start_date,
        measure_name,
        measure_code,
        case
            when measure_code like 'VOL_DEMAND%' then 'MVA' else measure_unit
        end as measure_unit,
        SUM(COALESCE(measure, 0)) as measure_value,
        seq_product_item_id as service_id,
        cast(NULL as string) as meter_id,
        cast(NULL as string) as postal_code
    from {{ ref('CalcAccrualsNeverBilledDetailCORE') }}
    group by
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
        case
            when measure_code like 'VOL_DEMAND%' then 'MVA' else measure_unit
        end,
        seq_product_item_id        

)

select '{{invocation_id}}' as run_id, *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
