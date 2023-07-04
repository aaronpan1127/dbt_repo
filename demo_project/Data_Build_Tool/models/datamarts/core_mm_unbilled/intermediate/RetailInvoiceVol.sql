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
        mrc.due_date AS in_mth_days,
        mrc.start_date AS bill_sdt,
        mrc.end_date AS bill_edt,
        mrc.name AS trans_type_code,
        mrc.description AS trans_type_desc,
        mrc.id AS invoice_header_id,
        CASE
            WHEN
                mrc.fuel_type = 'GAS' AND mrc.plan_item_name != 'DAILY_RETAIL'
                THEN 'Volume - Consumption'
            -- TimeClass.display_grouping_override currently has wrong values for these 
            WHEN mrc.time_class_name IN ('SUMMER', 'WINTER') THEN 'Volume - Climate Saver'
            WHEN
                (
                    mrc.plan_item_name = 'DEMAND_RETAIL'
                    OR upper(mrc.trans_description) LIKE '%DEMAND%'
                )
                THEN 'Volume - Demand'
            -- Treat capacity demand charges as primary for now
            WHEN
                mrc.plan_item_name = 'DAILY_RETAIL'
                THEN 'Billing Days - Supply Charge'
            -- Retailer payments for solar in South Australia
            WHEN
                (mrc.display_grouping_override = 'SOLAR')
                AND mrc.net_amount != 0
                THEN 'Volume - Solar'
            WHEN
                (mrc.display_grouping_override = 'SOLAR') AND mrc.net_amount = 0
                THEN 'Volume - Zero rated solar'
            WHEN
                mrc.display_grouping_override = 'CONTROLLED'
                THEN 'Volume - Controlled Load Usage'
            WHEN
                mrc.trans_description LIKE '%Any Time Usage%'
                THEN 'Volume - Anytime Usage'
            WHEN
                mrc.trans_description LIKE '%Off Peak%'
                THEN 'Volume - Off Peak'
            WHEN mrc.time_class_name = 'PEAK' THEN 'Volume - Peak'
            WHEN mrc.time_class_name = 'SHOULDER' THEN 'Volume - Shoulder'
            WHEN mrc.time_class_name = 'ANYTIME' THEN 'Volume - Anytime Usage'
            WHEN mrc.time_class_name = 'OFFPEAK' THEN 'Volume - Off Peak'
        END AS measure_name,
        CASE
            WHEN
                mrc.fuel_type = 'GAS' AND mrc.plan_item_name != 'DAILY_RETAIL'
                THEN 'VOL_CONSUM'
            -- TimeClass.display_grouping_override currently has wrong values for these, they are controlled load
            WHEN mrc.time_class_name IN ('SUMMER', 'WINTER') THEN 'VOL_CLIMAT'
            -- Treat capacity demand charges as primary for now
            WHEN
                (
                    mrc.plan_item_name = 'DEMAND_RETAIL'
                    OR upper(mrc.trans_description) LIKE '%DEMAND%'
                )
                THEN 'VOL_DEMAND'
            WHEN mrc.plan_item_name = 'DAILY_RETAIL' THEN 'DAY_SUPPLY'
            -- Retailer payments for solar in South Australia
            WHEN
                (mrc.display_grouping_override = 'SOLAR')
                AND mrc.net_amount != 0
                THEN 'VOL_SOLAR'
            WHEN
                (mrc.display_grouping_override = 'SOLAR') AND mrc.net_amount = 0
                THEN 'VOL_SZR_SO'
            WHEN mrc.display_grouping_override = 'CONTROLLED' THEN 'VOL_CONTRO'
            WHEN mrc.trans_description LIKE 'Any Time Usage%' THEN 'VOL_ANYTIM'
            WHEN mrc.trans_description LIKE '%Off Peak%' THEN 'VOL_OFFPEA'
            WHEN mrc.time_class_name = 'PEAK' THEN 'VOL_PEAK'
            WHEN mrc.time_class_name = 'SHOULDER' THEN 'VOL_SHOULD'
            WHEN mrc.time_class_name = 'OFFPEAK' THEN 'VOL_OFFPEA'
            WHEN mrc.time_class_name = 'ANYTIME' THEN 'VOL_ANYTIM'
        END AS measure_code,
        CASE
            WHEN mrc.plan_item_name = 'DAILY_RETAIL' THEN 'DAY'
            WHEN mrc.plan_item_name = 'DEMAND_RETAIL' THEN 'MVA'
            WHEN
                mrc.fuel_type = 'GAS' AND mrc.plan_item_name != 'DAILY_RETAIL'
                THEN 'GJ'
            WHEN
                mrc.display_grouping_override = 'SOLAR' AND mrc.net_amount != 0
                THEN 'MWh'
            ELSE 'MWh'
        END AS measure_unit,
        (datediff(mrc.end_date, mrc.start_date) + 1) AS billing_days,
        CASE
            WHEN
                mrc.fuel_type = 'ELECTRICITY'
                AND mrc.display_grouping_override = 'SOLAR'
                AND mrc.net_amount = 0
                THEN 'Y'
            ELSE 'N'
        END AS solar_zr,
        sign(mrc.net_amount) * abs(
            CASE
                -- plan_item_type_id = 35 ('Retailer Contribution')
                WHEN
                    (mrc.market_region = 'SA' AND mrc.plan_item_type_id = 35)
                    THEN 0
                WHEN
                    datediff(mrc.end_date, mrc.start_date) = 0
                    THEN mrc.quantity * mrc.multiplier
                ELSE
                    mrc.quantity
                    * mrc.multiplier
                    / (datediff(mrc.end_date, mrc.start_date) + 1)
            END
        ) AS daily_unit_quantity,
        CASE WHEN
            mrc.fuel_type = 'ELECTRICITY'
            AND mrc.display_grouping_override = 'SOLAR'
            AND mrc.net_amount = 0
            THEN -1 * abs(
                CASE
                    WHEN
                        (
                            mrc.market_region = 'SA'
                            AND mrc.plan_item_type_id = 35
                        )
                        THEN 0
                    WHEN
                        datediff(mrc.end_date, mrc.start_date) = 0
                        THEN mrc.quantity * mrc.multiplier
                    ELSE
                        mrc.quantity
                        * mrc.multiplier
                        / (datediff(mrc.end_date, mrc.start_date) + 1)
                END
            )
        ELSE 0 END AS daily_unit_quantity_zr,
        -- Invoice tables
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
    FROM
        {{ ref('RegistryCore') }} AS mrc
    WHERE
        mrc.trans_description NOT LIKE 'Concession%'
        AND mrc.trans_description != 'Transfer Decrease Cust Balance'
        AND mrc.trans_description != 'Account Adjustment'
        AND mrc.trans_description != 'Unplanned Interruption Adjustment'
        AND mrc.trans_description != 'Over Payment Refund Cheque'
        AND (coalesce(mrc.quantity, 0) != 0)
        -- Only include records with InvoiceLineItem.rate >= 0.5 when InvoiceLineItem.plan_item_type_id = 4 ('DAILY_RETAIL') for Electricity. This is to remove CL Supply Charge invoice line item when computing Billing Days metric.
        AND (
            (
                mrc.plan_item_name = 'DAILY_RETAIL'
                AND mrc.rate >= 0.5
                AND mrc.fuel_type = 'ELECTRICITY'
            )
            -- Include all rate when InvoiceLineItem.plan_item_type_id = 4 ('DAILY_RETAIL') for Gas
            OR (mrc.plan_item_name = 'DAILY_RETAIL' AND mrc.fuel_type = 'GAS')
            OR mrc.plan_item_name != 'DAILY_RETAIL'
        )

)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
