/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='adjustmentdiscount') }}


WITH source_data AS (

    SELECT DISTINCT
        fuel_type,
        market_region,
        market_sub_region,
        customer_segment,
        invoice_id,
        account_id AS seq_product_item_id,
        service_sdt,
        service_edt,
        invoice_date,
        usage_start_date AS bill_sdt,
        usage_end_date AS bill_edt,
        cntrl_load,
        measure_name,
        measure_code,
        measure_unit,
        discount_amount,
        solar_zr,
        in_mth_days,
        initcap(trans_description) AS trans_description,
        CASE
            WHEN
                datediff(usage_end_date, usage_start_date) > 0
                AND measure_code LIKE 'DIS%'
                THEN
                    (
                        discount_amount
                        / (datediff(usage_end_date, usage_start_date) + 1)
                    )
        END AS daily_disc_amt,
        CASE
            WHEN
                datediff(usage_end_date, usage_start_date) > 0
                AND measure_code LIKE 'EPPD%'
                THEN
                    (
                        discount_amount
                        / (datediff(usage_end_date, usage_start_date) + 1)
                    )
                    * (-1)
        END AS daily_elig_ppd_amt,
        CASE
            WHEN
                datediff(usage_end_date, usage_start_date) > 0
                AND measure_code LIKE 'PPD%'
                THEN
                    (
                        discount_amount
                        / (datediff(usage_end_date, usage_start_date) + 1)
                    )
                    * (-1)
        END AS daily_ppd_amt,
        (datediff(usage_end_date, usage_start_date) + 1) AS discount_days,
        date_part(
            'MONTH',
            date_add(
                usage_start_date,
                cast(
                    (datediff(usage_end_date, usage_start_date) + 1)
                    * (2 / 3) AS integer
                )
            )
        ) AS rmp_mth_no
    FROM (
        SELECT DISTINCT
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type AS customer_segment,
            i.id AS invoice_id,
            i.account_id,
            a.service_sdt,
            a.service_edt,
            date(i.posted_date) AS invoice_date,
            min(i.usage_start_date) AS usage_start_date,
            max(i.usage_end_date) AS usage_end_date,
            ili.description AS trans_description,
            CASE
                WHEN
                    tc.name IN ('SUMMER', 'WINTER')
                    OR tc.display_grouping_override LIKE 'CONTROLLED%'
                    THEN 'Y'
                ELSE 'N'
            END AS cntrl_load,
            -- , SUM(ili.net_amount) AS discount_amount --Change to Guaranteed Discount DAT 5560

            -- , SUM(CASE WHEN at2.includes_gst = 1 THEN ili.net_amount/11*10
            --            WHEN at2.includes_gst = 0 THEN ili.net_amount
            --            ELSE ili.net_amount
            --       END) AS discount_amount --Change to Guaranteed Discount DAT 5560, secondary change.  Required because Core's Includes GST flag is not always reliable.  It's been identified that, in certain circumstances, invoice line items have tax amounts.
            sum(CASE
                WHEN
                    ili.tax_amount = 0 AND ili.net_amount != 0
                    THEN ili.net_amount / 11 * 10
                WHEN
                    ili.tax_amount != 0 AND ili.net_amount != 0
                    THEN ili.net_amount
                ELSE 0
            --Change to Guaranteed Discount DAT 5560, secondary change.  
            END) AS discount_amount,
            'Discount - Guaranteed' AS measure_name,
            CASE
                WHEN a.fuel_type = 'ELECTRICITY' THEN 'DIS_SUPPLY' ELSE 'DIS'
            END AS measure_code,
            '$' AS measure_unit,
            CASE
                WHEN
                    a.fuel_type = 'ELECTRICITY'
                    AND tc.display_grouping_override = 'SOLAR'
                    AND sum(ili.net_amount) = 0
                    THEN 'Y'
                ELSE 'N'
            END AS solar_zr,
            i.due_date AS in_mth_days
        FROM
            (SELECT
                id,
                account_id,
                posted_date,
                usage_start_date,
                usage_end_date,
                due_date,
                type,
                rev_invoice_id
            FROM {{ source('structured_core_mm','invoice') }} WHERE adh_active_flag = 1) AS i
        INNER JOIN (
            SELECT
                id,
                account_id,
                tax_amount,
                net_amount,
                description,
                billable,
                adjustment_type_id,
                plan_item_type_id,
                invoice_id,
                discount_net,
                time_class_id
            FROM {{ source('structured_core_mm','invoicelineitem') }} WHERE adh_active_flag = 1
        ) AS ili
            ON i.id = ili.invoice_id
        INNER JOIN
            (SELECT
                id,
                adjustment_group_id,
                includes_gst,
                name
            FROM {{ source('structured_core_mm','adjustmenttype') }} WHERE adh_active_flag = 1)
            AS at2
            ON at2.id = ili.adjustment_type_id
        INNER JOIN
            (SELECT
                id,
                name
            FROM {{ source('structured_core_mm','adjustmentgroup') }} WHERE adh_active_flag = 1)
            AS ag
            ON ag.id = at2.adjustment_group_id
        -- Account Active Table
        INNER JOIN {{ ref('active_accounts_view') }} AS a ON a.account_id = ili.account_id
        LEFT JOIN
            (SELECT
                id,
                name,
                display_grouping_override
            FROM {{ source('structured_core_mm','timeclass') }} WHERE adh_active_flag = 1) AS tc
            ON tc.id = ili.time_class_id
        WHERE
            ili.billable = 1
            AND i.type != 'REVERSAL'
            -- covers both Guaranteed Discounts and Loyalty Credits
            --               AND ag.name = 'DISCOUNTS'  --Change to Guaranteed Discount DAT 5560
            --Change to Guaranteed Discount DAT 5560
            AND at2.name IN ('USAGE_DISC', 'USAGE_DISCCR', 'GD', 'GD_REV')
            AND (
                i.rev_invoice_id = 0
                OR i.rev_invoice_id = -1
                OR i.rev_invoice_id IS NULL
            )
            -- Considering only invoices posted before the Accrual period end date
            AND date(i.posted_date) <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
        GROUP BY
            i.id,
            i.account_id,
            i.posted_date,
            a.service_sdt,
            a.service_edt,
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type,
            ili.description,
            i.due_date,
            tc.name,
            tc.display_grouping_override

        UNION ALL


        SELECT
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type AS customer_segment,
            i.id AS invoice_id,
            i.account_id,
            a.service_sdt,
            a.service_edt,
            date(i.posted_date) AS invoice_date,
            min(i.usage_start_date) AS usage_start_date,
            max(i.usage_end_date) AS usage_end_date,
            ili.description AS trans_description,
            CASE
                WHEN
                    tc.name IN ('SUMMER', 'WINTER')
                    OR tc.display_grouping_override LIKE 'CONTROLLED%'
                    THEN 'Y'
                ELSE 'N'
            END AS cntrl_load,
            format_number(sum(ili.discount_net) / 11 * 10, '0.000')
            AS discount_amount,
            'Discount - Eligible PPD' AS measure_name,
            -- measure_code length is 10
            CASE
                WHEN a.fuel_type = 'ELECTRICITY' THEN 'EPPD_SUPPL' ELSE 'EPPD'
            END AS measure_code,
            '$' AS measure_unit,
            CASE
                WHEN
                    a.fuel_type = 'ELECTRICITY'
                    AND tc.display_grouping_override = 'SOLAR'
                    AND sum(ili.net_amount) = 0
                    THEN 'Y'
                ELSE 'N'
            END AS solar_zr,
            i.due_date AS in_mth_days

        FROM
            (SELECT
                id,
                account_id,
                posted_date,
                usage_start_date,
                usage_end_date,
                due_date,
                type,
                rev_invoice_id
            FROM {{ source('structured_core_mm','invoice') }} WHERE adh_active_flag = 1) AS i
        INNER JOIN (
            SELECT
                id,
                account_id,
                net_amount,
                description,
                billable,
                adjustment_type_id,
                plan_item_type_id,
                invoice_id,
                discount_net,
                time_class_id
            FROM {{ source('structured_core_mm','invoicelineitem') }} WHERE adh_active_flag = 1
        ) AS ili
            ON i.id = ili.invoice_id
        -- Account Active Table
        INNER JOIN {{ ref('active_accounts_view') }} AS a ON a.account_id = ili.account_id
        LEFT JOIN
            (SELECT
                id,
                name,
                display_grouping_override
            FROM {{ source('structured_core_mm','timeclass') }} WHERE adh_active_flag = 1) AS tc
            ON tc.id = ili.time_class_id
        WHERE
            ili.billable = 1
            AND i.type != 'REVERSAL'
            -- PPDs are for RETAIL_USAGE only
            AND ili.plan_item_type_id = 2
            -- exclude reversed invoices
            AND (
                i.rev_invoice_id = 0
                OR i.rev_invoice_id = -1
                OR i.rev_invoice_id IS NULL
            )
            -- Considering only invoices posted before the Accrual period end date
            AND date(i.posted_date) <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
        GROUP BY
            i.id,
            i.account_id,
            i.posted_date,
            a.service_sdt,
            a.service_edt,
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type,
            ili.description,
            i.due_date,
            tc.name,
            tc.display_grouping_override

        UNION ALL


        SELECT
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type AS customer_segment,
            i.id AS invoice_id,
            i.account_id,
            a.service_sdt,
            a.service_edt,
            date(i.posted_date) AS invoice_date,
            min(i.usage_start_date) AS usage_start_date,
            max(i.usage_end_date) AS usage_end_date,
            ili.description AS trans_description,
            CASE
                WHEN
                    tc.name IN ('SUMMER', 'WINTER')
                    OR tc.display_grouping_override LIKE 'CONTROLLED%'
                    THEN 'Y'
                ELSE 'N'
            END AS cntrl_load,
            format_number(sum(ili.discount_net) / 11 * 10, '0.000')
            AS discount_amount,
            'Discount - PPD' AS measure_name,
            CASE
                WHEN a.fuel_type = 'ELECTRICITY' THEN 'PPD_SUPPLY' ELSE 'PPD'
            END AS measure_code,
            '$' AS measure_unit,
            CASE
                WHEN
                    a.fuel_type = 'ELECTRICITY'
                    AND tc.display_grouping_override = 'SOLAR'
                    AND sum(ili.net_amount) = 0
                    THEN 'Y'
                ELSE 'N'
            END AS solar_zr,
            i.due_date AS in_mth_days

        FROM
            (SELECT
                id,
                account_id,
                posted_date,
                usage_start_date,
                usage_end_date,
                due_date,
                type,
                rev_invoice_id,
                ppd_adjustment_id
            FROM {{ source('structured_core_mm','invoice') }} WHERE adh_active_flag = 1) AS i
        INNER JOIN (
            SELECT
                id,
                account_id,
                net_amount,
                description,
                billable,
                adjustment_type_id,
                plan_item_type_id,
                invoice_id,
                discount_net,
                time_class_id
            FROM {{ source('structured_core_mm','invoicelineitem') }} WHERE adh_active_flag = 1
        ) AS ili
            ON i.id = ili.invoice_id
        INNER JOIN
            (SELECT
                id,
                invoice_desc
            FROM {{ source('structured_core_mm','adjustment') }} WHERE adh_active_flag = 1) AS adj
            ON adj.id = i.ppd_adjustment_id

        -- Account Active Table
        INNER JOIN {{ ref('active_accounts_view') }} AS a ON a.account_id = ili.account_id
        LEFT JOIN
            (SELECT
                id,
                name,
                display_grouping_override
            FROM {{ source('structured_core_mm','timeclass') }} WHERE adh_active_flag = 1) AS tc
            ON tc.id = ili.time_class_id
        WHERE
            ili.billable = 1
            AND i.type != 'REVERSAL'
            AND adj.invoice_desc LIKE 'DISCOUNT%'
            -- PPDs are for RETAIL_USAGE only
            AND ili.plan_item_type_id = 2
            -- exclude reversed invoices
            AND (
                i.rev_invoice_id = 0
                OR i.rev_invoice_id = -1
                OR i.rev_invoice_id IS NULL
            )
            -- Considering only invoices posted before the Accrual period end date
            AND date(i.posted_date) <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
        GROUP BY
            i.id,
            i.account_id,
            i.posted_date,
            a.service_sdt,
            a.service_edt,
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type,
            ili.description,
            i.due_date,
            tc.name,
            tc.display_grouping_override
    )
)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
