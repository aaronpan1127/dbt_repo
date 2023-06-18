/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='ephemeral') }}


WITH source_data AS (
    SELECT DISTINCT
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
        '$' AS measure_unit,
        mrc.due_date AS in_mth_days,
        mrc.start_date AS bill_sdt,
        mrc.end_date AS bill_edt,
        mrc.name AS trans_type_code,
        mrc.description AS trans_type_desc,
        mrc.id AS invoice_header_id,
        CASE
            WHEN
                mrc.fuel_type = 'GAS' AND mrc.plan_item_name != 'DAILY_RETAIL'
                THEN 'Revenue - Consumption'
            -- TimeClass.display_grouping_override currently has wrong values for these 
            WHEN mrc.time_class_name IN ('SUMMER', 'WINTER') THEN 'Revenue - Climate Saver'
            WHEN
                (
                    mrc.plan_item_name = 'DEMAND_RETAIL'
                    OR upper(mrc.trans_description) LIKE '%DEMAND%'
                )
                THEN 'Revenue - Demand'
            -- Treat capacity demand charges as primary for now
            WHEN
                mrc.plan_item_name = 'DAILY_RETAIL'
                THEN 'Revenue - Supply Charge'
            -- Retailer payments for solar in South Australia
            WHEN
                (
                    mrc.display_grouping_override = 'SOLAR'
                    OR mrc.plan_item_name = 'CONTRIBUTION'
                )
                THEN 'Revenue - Solar'
            WHEN
                mrc.display_grouping_override = 'CONTROLLED'
                THEN 'Revenue - Controlled Load Usage'
            WHEN
                mrc.trans_description LIKE '%Any Time Usage%'
                THEN 'Revenue - Anytime Usage'
            WHEN
                mrc.trans_description LIKE '%Off Peak%'
                THEN 'Revenue - Off Peak'
            WHEN mrc.time_class_name = 'PEAK' THEN 'Revenue - Peak'
            WHEN mrc.time_class_name = 'SHOULDER' THEN 'Revenue - Shoulder'
            WHEN mrc.time_class_name = 'ANYTIME' THEN 'Revenue - Anytime Usage'
            WHEN mrc.time_class_name = 'OFFPEAK' THEN 'Revenue - Off Peak'
        END AS measure_name,
        CASE
            WHEN
                mrc.fuel_type = 'GAS' AND mrc.plan_item_name != 'DAILY_RETAIL'
                THEN 'REV_CONSUM'
            -- TimeClass.display_grouping_override currently has wrong values for these, they are controlled load
            WHEN mrc.time_class_name IN ('SUMMER', 'WINTER') THEN 'REV_CLIMAT'
            -- Treat capacity demand charges as primary for now
            WHEN
                (
                    mrc.plan_item_name = 'DEMAND_RETAIL'
                    OR upper(mrc.trans_description) LIKE '%DEMAND%'
                )
                THEN 'REV_DEMAND'
            WHEN mrc.plan_item_name = 'DAILY_RETAIL' THEN 'REV_SUPPLY'
            -- Retailer payments for solar in South 
            WHEN
                (
                    mrc.display_grouping_override = 'SOLAR'
                    OR mrc.plan_item_name = 'CONTRIBUTION'
                )
                THEN 'REV_SOLAR'
            WHEN mrc.display_grouping_override = 'CONTROLLED' THEN 'REV_CONTRO'
            WHEN mrc.trans_description LIKE 'Any Time Usage%' THEN 'REV_ANYTIM'
            WHEN mrc.trans_description LIKE '%Off Peak%' THEN 'REV_OFFPEA'
            WHEN mrc.time_class_name = 'PEAK' THEN 'REV_PEAK'
            WHEN mrc.time_class_name = 'SHOULDER' THEN 'REV_SHOULD'
            WHEN mrc.time_class_name = 'OFFPEAK' THEN 'REV_OFFPEA'
            WHEN mrc.time_class_name = 'ANYTIME' THEN 'REV_ANYTIM'
        END AS measure_code,
        (datediff(mrc.end_date, mrc.start_date) + 1) AS billing_days,
        CASE
            WHEN
                mrc.fuel_type = 'ELECTRICITY'
                AND mrc.display_grouping_override = 'SOLAR'
                AND mrc.net_amount = 0
                THEN 'Y'
            ELSE 'N'
        END AS solar_zr,
        CASE
            WHEN
                datediff(mrc.end_date, mrc.start_date) = 0
                THEN mrc.quantity * mrc.multiplier
            ELSE
                mrc.quantity
                * mrc.multiplier
                / (datediff(mrc.end_date, mrc.start_date) + 1)
        END
        * mrc.rate AS daily_amt,
        date_part(
            'MONTH',
            date_add(
                mrc.start_date,
                cast(
                    (datediff(mrc.end_date, mrc.start_date) + 1)
                    * (2 / 3) AS integer
                )
            )
        ) AS rmp_mth_no
    -- Invoice tables
    FROM
        {{ ref('RegistryCore') }} AS mrc
    WHERE
        mrc.trans_description NOT LIKE 'Concession%'
        AND mrc.trans_description != 'Transfer Decrease Cust Balance'
        AND mrc.trans_description != 'Account Adjustment'
        AND mrc.trans_description != 'Unplanned Interruption Adjustment'
        AND mrc.trans_description != 'Over Payment Refund Cheque'
        AND (coalesce(mrc.quantity, 0) != 0)

)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
