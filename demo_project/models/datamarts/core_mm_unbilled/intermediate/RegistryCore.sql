/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='registrycore') }}

WITH source_data AS (

    SELECT DISTINCT
        aa.account_id AS seq_product_item_id,
        aa.market_region,
        aa.market_sub_region,
        'MM' AS market_segment,
        aa.customer_type AS customer_segment,
        aa.fuel_type,
        u.identifier_all AS meter_id,
        aa.service_sdt,
        aa.service_edt,
        ilt.id,
        ilt.invoice_id,
        ilt.plan_item_type_id,
        ilt.adjustment_id,
        ilt.start_date,
        ilt.end_date,
        ilt.net_amount,
        ilt.quantity,
        ilt.multiplier,
        ilt.rate,
        ilt.loss_factor,
        ilt.discount_net,
        inv.discount_amount,
        inv.allocated_amount,
        inv.due_date,
        au.vacant_site,
        pit.name AS plan_item_name,
        tc.name AS time_class_name,
        tc.display_grouping_override,
        lic.name,
        lic.description,
        inv.rev_invoice_id,
        inv.posted_date,
        initcap(ilt.description) AS trans_description,
        CASE
            WHEN tc.name IN ('SUMMER', 'WINTER') OR tc.display_grouping_override LIKE 'CONTROLLED%' THEN 'Y' ELSE 'N'
        END AS cntrl_load
    FROM
        {{ ref('active_accounts_view') }} AS aa
    -- Invoice tables
        JOIN
        (SELECT * FROM {{ source('structured_core_mm', 'invoicelineitem') }} WHERE adh_active_flag = 1) AS ilt
        ON aa.account_id = ilt.account_id
        JOIN
        (SELECT
            id,
            name,
            description
        FROM {{ source('structured_core_mm', 'lineitemcategory') }} WHERE adh_active_flag = 1) AS lic
        ON lic.id = ilt.line_item_category_id
    LEFT JOIN
        (SELECT
            id,
            name
        FROM {{ source('structured_core_mm', 'planitemtype') }} WHERE adh_active_flag = 1) AS pit
        ON pit.id = ilt.plan_item_type_id
    LEFT JOIN
        (SELECT
            id,
            name,
            display_grouping_override
        FROM {{ source('structured_core_mm', 'timeclass') }} WHERE adh_active_flag = 1) AS tc
        ON tc.id = ilt.time_class_id
    LEFT JOIN
        (SELECT
            id,
            ppd_adjustment_id,
            discount_amount,
            due_date,
            allocated_amount,
            rev_invoice_id,
            posted_date
        FROM {{ source('structured_core_mm', 'invoice') }} WHERE adh_active_flag = 1) AS inv
        ON inv.id = ilt.invoice_id
    -- Utility tables
        JOIN
        (SELECT
            id,
            utility_id,
            vacant_site,
            account_id,
            access_detail
        FROM {{ source('structured_core_mm', 'accountutility') }} WHERE adh_active_flag = 1) AS au
        ON au.account_id = aa.account_id
        JOIN
        (SELECT
            id,
            identifier_all,
            cust_class_code,
            utility_network_id,
            jurisdiction_id
        FROM {{ source('structured_core_mm', 'utility') }} WHERE adh_active_flag = 1) AS u
        ON u.id = au.utility_id

    WHERE
        -- Invoice is billable
        ilt.billable = 1
        -- Exclude reversed invoices
        AND (inv.rev_invoice_id = 0 OR inv.rev_invoice_id = -1 OR inv.rev_invoice_id IS NULL)
        AND (ilt.line_item_category_id = 1 OR (ilt.line_item_category_id = 2 AND pit.id = 36))
        -- Considering only invoices posted before the Accrual period end date
        AND to_date(inv.posted_date) <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
-- AND (to_date(ilt.end_date) >= "${Month_Start_Date}" OR to_date(ilt.start_date) <=  "${UnbilledAccrualPeriodEndDate}")
)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
