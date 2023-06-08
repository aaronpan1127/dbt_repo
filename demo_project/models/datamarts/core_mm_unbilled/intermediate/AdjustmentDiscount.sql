/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table',alias='adjustmentdiscount') }}


with source_data as (

    select distinct
        fuel_type,
        market_region,
        market_sub_region,
        customer_segment,
        invoice_id,
        account_id as seq_product_item_id,
        service_sdt,
        service_edt,
        invoice_date,
        usage_start_date as bill_sdt,
        usage_end_date as bill_edt,
        cntrl_load,
        measure_name,
        measure_code,
        measure_unit,
        discount_amount,
        solar_zr,
        in_mth_days,
        initcap(trans_description) as trans_description,
        case
            when
                datediff(usage_end_date, usage_start_date) > 0
                and measure_code like 'DIS%'
                then
                    (
                        discount_amount
                        / (datediff(usage_end_date, usage_start_date) + 1)
                    )
        end as daily_disc_amt,
        case
            when
                datediff(usage_end_date, usage_start_date) > 0
                and measure_code like 'EPPD%'
                then
                    (
                        discount_amount
                        / (datediff(usage_end_date, usage_start_date) + 1)
                    )
                    * (-1)
        end as daily_elig_ppd_amt,
        case
            when
                datediff(usage_end_date, usage_start_date) > 0
                and measure_code like 'PPD%'
                then
                    (
                        discount_amount
                        / (datediff(usage_end_date, usage_start_date) + 1)
                    )
                    * (-1)
        end as daily_ppd_amt,
        (datediff(usage_end_date, usage_start_date) + 1) as discount_days,
        date_part(
            'MONTH',
            date_add(
                usage_start_date,
                cast(
                    (datediff(usage_end_date, usage_start_date) + 1)
                    * (2 / 3) as integer
                )
            )
        ) as rmp_mth_no
    from (
        select distinct
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type as customer_segment,
            i.id as invoice_id,
            i.account_id,
            a.service_sdt,
            a.service_edt,
            date(i.posted_date) as invoice_date,
            min(i.usage_start_date) as usage_start_date,
            max(i.usage_end_date) as usage_end_date,
            ili.description as trans_description,
            case
                when
                    tc.name in ('SUMMER', 'WINTER')
                    or tc.display_grouping_override like 'CONTROLLED%'
                    then 'Y'
                else 'N'
            end as cntrl_load,
            -- , SUM(ili.net_amount) AS discount_amount --Change to Guaranteed Discount DAT 5560

            -- , SUM(CASE WHEN at2.includes_gst = 1 THEN ili.net_amount/11*10
            --            WHEN at2.includes_gst = 0 THEN ili.net_amount
            --            ELSE ili.net_amount
            --       END) AS discount_amount --Change to Guaranteed Discount DAT 5560, secondary change.  Required because Core's Includes GST flag is not always reliable.  It's been identified that, in certain circumstances, invoice line items have tax amounts.
            sum(case
                when
                    ili.tax_amount = 0 and ili.net_amount != 0
                    then ili.net_amount / 11 * 10
                when
                    ili.tax_amount != 0 and ili.net_amount != 0
                    then ili.net_amount
                else 0
            --Change to Guaranteed Discount DAT 5560, secondary change.  
            end) as discount_amount,
            'Discount - Guaranteed' as measure_name,
            case
                when a.fuel_type = 'ELECTRICITY' then 'DIS_SUPPLY' else 'DIS'
            end as measure_code,
            '$' as measure_unit,
            case
                when
                    a.fuel_type = 'ELECTRICITY'
                    and tc.display_grouping_override = 'SOLAR'
                    and sum(ili.net_amount) = 0
                    then 'Y'
                else 'N'
            end as solar_zr,
            i.due_date as in_mth_days
        from
            (select
                id,
                account_id,
                posted_date,
                usage_start_date,
                usage_end_date,
                due_date,
                type,
                rev_invoice_id
            from {{ source('structured_core_mm','invoice') }} where adh_active_flag = 1) as i
        inner join (
            select
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
            from {{ source('structured_core_mm','invoicelineitem') }} where adh_active_flag = 1
        ) as ili
            on i.id = ili.invoice_id
        inner join
            (select
                id,
                adjustment_group_id,
                includes_gst,
                name
            from {{ source('structured_core_mm','adjustmenttype') }} where adh_active_flag = 1)
                as at2
            on at2.id = ili.adjustment_type_id
        inner join
            (select
                id,
                name
            from {{ source('structured_core_mm','adjustmentgroup') }} where adh_active_flag = 1)
                as ag
            on ag.id = at2.adjustment_group_id
        -- Account Active Table
        inner join {{ ref('active_accounts_view') }} as a on a.account_id = ili.account_id
        left join
            (select
                id,
                name,
                display_grouping_override
            from {{ source('structured_core_mm','timeclass') }} where adh_active_flag = 1) as tc
            on tc.id = ili.time_class_id
        where
            ili.billable = 1
            and i.type != 'REVERSAL'
            -- covers both Guaranteed Discounts and Loyalty Credits
            --               AND ag.name = 'DISCOUNTS'  --Change to Guaranteed Discount DAT 5560
            --Change to Guaranteed Discount DAT 5560
            and at2.name in ('USAGE_DISC', 'USAGE_DISCCR', 'GD', 'GD_REV')
            and (
                i.rev_invoice_id = 0
                or i.rev_invoice_id = -1
                or i.rev_invoice_id is null
            )
            -- Considering only invoices posted before the Accrual period end date
            and date(i.posted_date) <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
        group by
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

        union all


        select
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type as customer_segment,
            i.id as invoice_id,
            i.account_id,
            a.service_sdt,
            a.service_edt,
            date(i.posted_date) as invoice_date,
            min(i.usage_start_date) as usage_start_date,
            max(i.usage_end_date) as usage_end_date,
            ili.description as trans_description,
            case
                when
                    tc.name in ('SUMMER', 'WINTER')
                    or tc.display_grouping_override like 'CONTROLLED%'
                    then 'Y'
                else 'N'
            end as cntrl_load,
            format_number(sum(ili.discount_net) / 11 * 10, '0.000')
                as discount_amount,
            'Discount - Eligible PPD' as measure_name,
            -- measure_code length is 10
            case
                when a.fuel_type = 'ELECTRICITY' then 'EPPD_SUPPL' else 'EPPD'
            end as measure_code,
            '$' as measure_unit,
            case
                when
                    a.fuel_type = 'ELECTRICITY'
                    and tc.display_grouping_override = 'SOLAR'
                    and sum(ili.net_amount) = 0
                    then 'Y'
                else 'N'
            end as solar_zr,
            i.due_date as in_mth_days

        from
            (select
                id,
                account_id,
                posted_date,
                usage_start_date,
                usage_end_date,
                due_date,
                type,
                rev_invoice_id
            from {{ source('structured_core_mm','invoice') }} where adh_active_flag = 1) as i
        inner join (
            select
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
            from {{ source('structured_core_mm','invoicelineitem') }} where adh_active_flag = 1
        ) as ili
            on i.id = ili.invoice_id
        -- Account Active Table
        inner join {{ ref('active_accounts_view') }} as a on a.account_id = ili.account_id
        left join
            (select
                id,
                name,
                display_grouping_override
            from {{ source('structured_core_mm','timeclass') }} where adh_active_flag = 1) as tc
            on tc.id = ili.time_class_id
        where
            ili.billable = 1
            and i.type != 'REVERSAL'
            -- PPDs are for RETAIL_USAGE only
            and ili.plan_item_type_id = 2
            -- exclude reversed invoices
            and (
                i.rev_invoice_id = 0
                or i.rev_invoice_id = -1
                or i.rev_invoice_id is null
            )
            -- Considering only invoices posted before the Accrual period end date
            and date(i.posted_date) <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
        group by
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

        union all


        select
            a.fuel_type,
            a.market_region,
            a.market_sub_region,
            a.customer_type as customer_segment,
            i.id as invoice_id,
            i.account_id,
            a.service_sdt,
            a.service_edt,
            date(i.posted_date) as invoice_date,
            min(i.usage_start_date) as usage_start_date,
            max(i.usage_end_date) as usage_end_date,
            ili.description as trans_description,
            case
                when
                    tc.name in ('SUMMER', 'WINTER')
                    or tc.display_grouping_override like 'CONTROLLED%'
                    then 'Y'
                else 'N'
            end as cntrl_load,
            format_number(sum(ili.discount_net) / 11 * 10, '0.000')
                as discount_amount,
            'Discount - PPD' as measure_name,
            case
                when a.fuel_type = 'ELECTRICITY' then 'PPD_SUPPLY' else 'PPD'
            end as measure_code,
            '$' as measure_unit,
            case
                when
                    a.fuel_type = 'ELECTRICITY'
                    and tc.display_grouping_override = 'SOLAR'
                    and sum(ili.net_amount) = 0
                    then 'Y'
                else 'N'
            end as solar_zr,
            i.due_date as in_mth_days

        from
            (select
                id,
                account_id,
                posted_date,
                usage_start_date,
                usage_end_date,
                due_date,
                type,
                rev_invoice_id,
                ppd_adjustment_id
            from {{ source('structured_core_mm','invoice') }} where adh_active_flag = 1) as i
        inner join (
            select
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
            from {{ source('structured_core_mm','invoicelineitem') }} where adh_active_flag = 1
        ) as ili
            on i.id = ili.invoice_id
        inner join
            (select
                id,
                invoice_desc
            from {{ source('structured_core_mm','adjustment') }} where adh_active_flag = 1) as adj
            on adj.id = i.ppd_adjustment_id

        -- Account Active Table
        inner join {{ ref('active_accounts_view') }} as a on a.account_id = ili.account_id
        left join
            (select
                id,
                name,
                display_grouping_override
            from {{ source('structured_core_mm','timeclass') }} where adh_active_flag = 1) as tc
            on tc.id = ili.time_class_id
        where
            ili.billable = 1
            and i.type != 'REVERSAL'
            and adj.invoice_desc like 'DISCOUNT%'
            -- PPDs are for RETAIL_USAGE only
            and ili.plan_item_type_id = 2
            -- exclude reversed invoices
            and (
                i.rev_invoice_id = 0
                or i.rev_invoice_id = -1
                or i.rev_invoice_id is null
            )
            -- Considering only invoices posted before the Accrual period end date
            and date(i.posted_date) <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
        group by
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

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
