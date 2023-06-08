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
        mrc.due_date as in_mth_days,
        mrc.start_date as bill_sdt,
        mrc.end_date as bill_edt,
        mrc.name as trans_type_code,
        mrc.description as trans_type_desc,
        mrc.id as invoice_header_id,
        case
            when
                mrc.fuel_type = 'GAS' and mrc.plan_item_name != 'DAILY_RETAIL'
                then 'Volume - Consumption'
            when mrc.time_class_name in ('SUMMER', 'WINTER') then 'Volume - Climate Saver' -- TimeClass.display_grouping_override currently has wrong values for these 
            when
                (
                    mrc.plan_item_name = 'DEMAND_RETAIL'
                    or UPPER(mrc.trans_description) like '%DEMAND%'
                )
                then 'Volume - Demand'
            -- Treat capacity demand charges as primary for now
            when
                mrc.plan_item_name = 'DAILY_RETAIL'
                then 'Billing Days - Supply Charge'
            -- Retailer payments for solar in South Australia
            when
                (mrc.display_grouping_override = 'SOLAR')
                and mrc.net_amount != 0
                then 'Volume - Solar'
            when
                (mrc.display_grouping_override = 'SOLAR') and mrc.net_amount = 0
                then 'Volume - Zero rated solar'
            when
                mrc.display_grouping_override = 'CONTROLLED'
                then 'Volume - Controlled Load Usage'
            when
                mrc.trans_description like '%Any Time Usage%'
                then 'Volume - Anytime Usage'
            when
                mrc.trans_description like '%Off Peak%'
                then 'Volume - Off Peak'
            when mrc.time_class_name = 'PEAK' then 'Volume - Peak'
            when mrc.time_class_name = 'SHOULDER' then 'Volume - Shoulder'
            when mrc.time_class_name = 'ANYTIME' then 'Volume - Anytime Usage'
            when mrc.time_class_name = 'OFFPEAK' then 'Volume - Off Peak'
        end as measure_name,
        case
            when
                mrc.fuel_type = 'GAS' and mrc.plan_item_name != 'DAILY_RETAIL'
                then 'VOL_CONSUM'
            when mrc.time_class_name in ('SUMMER', 'WINTER') then 'VOL_CLIMAT' -- TimeClass.display_grouping_override currently has wrong values for these, they are controlled load
            -- Treat capacity demand charges as primary for now
            when
                (
                    mrc.plan_item_name = 'DEMAND_RETAIL'
                    or UPPER(mrc.trans_description) like '%DEMAND%'
                )
                then 'VOL_DEMAND'
            when mrc.plan_item_name = 'DAILY_RETAIL' then 'DAY_SUPPLY'
            -- Retailer payments for solar in South Australia
            when
                (mrc.display_grouping_override = 'SOLAR')
                and mrc.net_amount != 0
                then 'VOL_SOLAR'
            when
                (mrc.display_grouping_override = 'SOLAR') and mrc.net_amount = 0
                then 'VOL_SZR_SO'
            when mrc.display_grouping_override = 'CONTROLLED' then 'VOL_CONTRO'
            when mrc.trans_description like 'Any Time Usage%' then 'VOL_ANYTIM'
            when mrc.trans_description like '%Off Peak%' then 'VOL_OFFPEA'
            when mrc.time_class_name = 'PEAK' then 'VOL_PEAK'
            when mrc.time_class_name = 'SHOULDER' then 'VOL_SHOULD'
            when mrc.time_class_name = 'OFFPEAK' then 'VOL_OFFPEA'
            when mrc.time_class_name = 'ANYTIME' then 'VOL_ANYTIM'
        end as measure_code,
        case
            when mrc.plan_item_name = 'DAILY_RETAIL' then 'DAY'
            when mrc.plan_item_name = 'DEMAND_RETAIL' then 'MVA'
            when
                mrc.fuel_type = 'GAS' and mrc.plan_item_name != 'DAILY_RETAIL'
                then 'GJ'
            when
                mrc.display_grouping_override = 'SOLAR' and mrc.net_amount != 0
                then 'MWh'
            else 'MWh'
        end as measure_unit,
        (DATEDIFF(mrc.end_date, mrc.start_date) + 1) as billing_days,
        case
            when
                mrc.fuel_type = 'ELECTRICITY'
                and mrc.display_grouping_override = 'SOLAR'
                and mrc.net_amount = 0
                then 'Y'
            else 'N'
        end as solar_zr,
        SIGN(mrc.net_amount) * ABS(
            case
                -- plan_item_type_id = 35 ('Retailer Contribution')
                when
                    (mrc.market_region = 'SA' and mrc.plan_item_type_id = 35)
                    then 0
                when
                    DATEDIFF(mrc.end_date, mrc.start_date) = 0
                    then mrc.quantity * mrc.multiplier
                else
                    mrc.quantity
                    * mrc.multiplier
                    / (DATEDIFF(mrc.end_date, mrc.start_date) + 1)
            end
        ) as daily_unit_quantity,
        case when
            mrc.fuel_type = 'ELECTRICITY'
            and mrc.display_grouping_override = 'SOLAR'
            and mrc.net_amount = 0
            then -1 * ABS(
                case
                    when
                        (
                            mrc.market_region = 'SA'
                            and mrc.plan_item_type_id = 35
                        )
                        then 0
                    when
                        DATEDIFF(mrc.end_date, mrc.start_date) = 0
                        then mrc.quantity * mrc.multiplier
                    else
                        mrc.quantity
                        * mrc.multiplier
                        / (DATEDIFF(mrc.end_date, mrc.start_date) + 1)
                end
            )
        else 0 end as daily_unit_quantity_zr,
        -- Invoice tables
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
    from
        {{ ref('RegistryCore') }} as mrc
    where
        mrc.trans_description not like 'Concession%'
        and mrc.trans_description != 'Transfer Decrease Cust Balance'
        and mrc.trans_description != 'Account Adjustment'
        and mrc.trans_description != 'Unplanned Interruption Adjustment'
        and mrc.trans_description != 'Over Payment Refund Cheque'
        and (COALESCE(mrc.quantity, 0) != 0)
        -- Only include records with InvoiceLineItem.rate >= 0.5 when InvoiceLineItem.plan_item_type_id = 4 ('DAILY_RETAIL') for Electricity. This is to remove CL Supply Charge invoice line item when computing Billing Days metric.
        and (
            (
                mrc.plan_item_name = 'DAILY_RETAIL'
                and mrc.rate >= 0.5
                and mrc.fuel_type = 'ELECTRICITY'
            )
            -- Include all rate when InvoiceLineItem.plan_item_type_id = 4 ('DAILY_RETAIL') for Gas
            or (mrc.plan_item_name = 'DAILY_RETAIL' and mrc.fuel_type = 'GAS')
            or mrc.plan_item_name != 'DAILY_RETAIL'
        )

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
