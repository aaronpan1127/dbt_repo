/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='ephemeral') }}


with source_data as (
    select distinct
        mrc.fuel_type,
        mrc.market_region,
        mrc.market_sub_region,
        mrc.customer_segment,
        mrc.cntrl_load,
        mrc.service_sdt,
        mrc.service_edt,
        mrc.seq_product_item_id,
        mrc.net_amount,
        mrc.trans_description,
        mrc.adjustment_id,
        mrc.meter_id,
        '$' as measure_unit,
        mrc.due_date as in_mth_days,
        mrc.start_date as bill_sdt,
        mrc.end_date as bill_edt,
        mrc.name as trans_type_code,
        mrc.description as trans_type_desc,
        mrc.id as invoice_header_id,
        case
            when
                mrc.fuel_type = 'GAS' and mrc.plan_item_name != 'DAILY_RETAIL'
                then 'Revenue - Consumption'
            when mrc.time_class_name in ('SUMMER', 'WINTER') then 'Revenue - Climate Saver' -- TimeClass.display_grouping_override currently has wrong values for these 
            when
                (
                    mrc.plan_item_name = 'DEMAND_RETAIL'
                    or UPPER(mrc.trans_description) like '%DEMAND%'
                )
                then 'Revenue - Demand'
            -- Treat capacity demand charges as primary for now
            when
                mrc.plan_item_name = 'DAILY_RETAIL'
                then 'Revenue - Supply Charge'
            -- Retailer payments for solar in South Australia
            when
                (
                    mrc.display_grouping_override = 'SOLAR'
                    or mrc.plan_item_name = 'CONTRIBUTION'
                )
                then 'Revenue - Solar'
            when
                mrc.display_grouping_override = 'CONTROLLED'
                then 'Revenue - Controlled Load Usage'
            when
                mrc.trans_description like '%Any Time Usage%'
                then 'Revenue - Anytime Usage'
            when
                mrc.trans_description like '%Off Peak%'
                then 'Revenue - Off Peak'
            when mrc.time_class_name = 'PEAK' then 'Revenue - Peak'
            when mrc.time_class_name = 'SHOULDER' then 'Revenue - Shoulder'
            when mrc.time_class_name = 'ANYTIME' then 'Revenue - Anytime Usage'
            when mrc.time_class_name = 'OFFPEAK' then 'Revenue - Off Peak'
        end as measure_name,
        case
            when
                mrc.fuel_type = 'GAS' and mrc.plan_item_name != 'DAILY_RETAIL'
                then 'REV_CONSUM'
            when mrc.time_class_name in ('SUMMER', 'WINTER') then 'REV_CLIMAT' -- TimeClass.display_grouping_override currently has wrong values for these, they are controlled load
            -- Treat capacity demand charges as primary for now
            when
                (
                    mrc.plan_item_name = 'DEMAND_RETAIL'
                    or UPPER(mrc.trans_description) like '%DEMAND%'
                )
                then 'REV_DEMAND'
            when mrc.plan_item_name = 'DAILY_RETAIL' then 'REV_SUPPLY'
            -- Retailer payments for solar in South 
            when
                (
                    mrc.display_grouping_override = 'SOLAR'
                    or mrc.plan_item_name = 'CONTRIBUTION'
                )
                then 'REV_SOLAR'
            when mrc.display_grouping_override = 'CONTROLLED' then 'REV_CONTRO'
            when mrc.trans_description like 'Any Time Usage%' then 'REV_ANYTIM'
            when mrc.trans_description like '%Off Peak%' then 'REV_OFFPEA'
            when mrc.time_class_name = 'PEAK' then 'REV_PEAK'
            when mrc.time_class_name = 'SHOULDER' then 'REV_SHOULD'
            when mrc.time_class_name = 'OFFPEAK' then 'REV_OFFPEA'
            when mrc.time_class_name = 'ANYTIME' then 'REV_ANYTIM'
        end as measure_code,
        (DATEDIFF(mrc.end_date, mrc.start_date) + 1) as billing_days,
        case
            when
                mrc.fuel_type = 'ELECTRICITY'
                and mrc.display_grouping_override = 'SOLAR'
                and mrc.net_amount = 0
                then 'Y'
            else 'N'
        end as solar_zr,
        case
            when
                DATEDIFF(mrc.end_date, mrc.start_date) = 0
                then mrc.quantity * mrc.multiplier
            else
                mrc.quantity
                * mrc.multiplier
                / (DATEDIFF(mrc.end_date, mrc.start_date) + 1)
        end
        * mrc.rate as daily_amt,
        DATE_PART(
            'MONTH',
            DATE_ADD(
                mrc.start_date,
                CAST(
                    (DATEDIFF(mrc.end_date, mrc.start_date) + 1)
                    * (2 / 3) as integer
                )
            )
        ) as rmp_mth_no
    -- Invoice tables
    from
        {{ ref('RegistryCore') }} as mrc
    where
        mrc.trans_description not like 'Concession%'
        and mrc.trans_description != 'Transfer Decrease Cust Balance'
        and mrc.trans_description != 'Account Adjustment'
        and mrc.trans_description != 'Unplanned Interruption Adjustment'
        and mrc.trans_description != 'Over Payment Refund Cheque'
        and (COALESCE(mrc.quantity, 0) != 0)

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
