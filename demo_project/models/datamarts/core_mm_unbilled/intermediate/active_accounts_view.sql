/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='ephemeral') }}


with source_data as (

    select distinct
        a.id as account_id,
        st.name as fuel_type,
        j.name as market_region,
        un.description as market_sub_region,
        jas.name as customer_type,
        a.creation_date as creation_date,
        COALESCE(a.commence_date, '1900-01-01') as service_sdt,
        COALESCE(a.closed_date, '9999-12-31') as service_edt,
        UPPER(sg.name) as account_status
    from (
        select
            id,
            service_type_id,
            adh_active_flag,
            journal_segment_id,
            closed_date,
            commence_date,
            creation_date,
            curr_status_sid
        from structured_core_mm.account
        where adh_active_flag = 1
        -- Considering account with commence_date populated
        and commence_date is not null
    ) as a
    inner join
        (select
            account_id,
            utility_id
        from {{ source('structured_core_mm', 'accountutility') }} where adh_active_flag = 1) as au
        on au.account_id = a.id
    inner join
        (select
            id,
            utility_network_id,
            jurisdiction_id
        from {{ source('structured_core_mm', 'utility') }} where adh_active_flag = 1) as u
        on u.id = au.utility_id
    inner join
        (select
            id,
            description
        from {{ source('structured_core_mm', 'utilitynetwork') }}  where adh_active_flag = 1) as un
        on un.id = u.utility_network_id
    inner join
        (select
            id,
            name
        from {{ source('structured_core_mm', 'jurisdiction') }} where adh_active_flag = 1) as j
        on j.id = u.jurisdiction_id
    inner join
        (select
            id,
            name
        from {{ source('structured_core_mm', 'servicetype') }} where adh_active_flag = 1) as st
        on st.id = a.service_type_id
    inner join
        (
            select
                id,
                name
            from {{ source('structured_core_mm', 'journalaccountsegment') }}
            where adh_active_flag = 1
        ) as jas
        on jas.id = a.journal_segment_id
    inner join
        (select
            id,
            status_group_id
        from {{ source('structured_core_mm', 'eventtype') }} where adh_active_flag = 1) as et
        on et.id = a.curr_status_sid
    inner join
        (select
            id,
            name
        from {{ source('structured_core_mm', 'statusgroup') }} where adh_active_flag = 1) as sg
        on sg.id = et.status_group_id
    where
        a.commence_date
        <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
        and COALESCE(a.closed_date, '9999-12-31')
        >= '{{ var('Month_Start_Date','2022-04-01T00:00:00') }}'
        -- Considering only Account created before the Accrual period end date
        and TO_DATE(a.creation_date)
        <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
