/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='ephemeral') }}


WITH source_data AS (

    SELECT DISTINCT
        a.id AS account_id,
        st.name AS fuel_type,
        j.name AS market_region,
        un.description AS market_sub_region,
        jas.name AS customer_type,
        a.creation_date AS creation_date,
        coalesce(a.commence_date, '1900-01-01') AS service_sdt,
        coalesce(a.closed_date, '9999-12-31') AS service_edt,
        upper(sg.name) AS account_status
    FROM (
        SELECT
            id,
            service_type_id,
            adh_active_flag,
            journal_segment_id,
            closed_date,
            commence_date,
            creation_date,
            curr_status_sid
        FROM {{ source('structured_core_mm', 'account') }}
        WHERE adh_active_flag = 1
        -- Considering account with commence_date populated
        AND commence_date IS NOT NULL
    ) AS a
    INNER JOIN
        (SELECT
            account_id,
            utility_id
        FROM {{ source('structured_core_mm', 'accountutility') }} WHERE adh_active_flag = 1) AS au
        ON au.account_id = a.id
    INNER JOIN
        (SELECT
            id,
            utility_network_id,
            jurisdiction_id
        FROM {{ source('structured_core_mm', 'utility') }} WHERE adh_active_flag = 1) AS u
        ON u.id = au.utility_id
    INNER JOIN
        (SELECT
            id,
            description
        FROM {{ source('structured_core_mm', 'utilitynetwork') }} WHERE adh_active_flag = 1) AS un
        ON un.id = u.utility_network_id
    INNER JOIN
        (SELECT
            id,
            name
        FROM {{ source('structured_core_mm', 'jurisdiction') }} WHERE adh_active_flag = 1) AS j
        ON j.id = u.jurisdiction_id
    INNER JOIN
        (SELECT
            id,
            name
        FROM {{ source('structured_core_mm', 'servicetype') }} WHERE adh_active_flag = 1) AS st
        ON st.id = a.service_type_id
    INNER JOIN
        (
            SELECT
                id,
                name
            FROM {{ source('structured_core_mm', 'journalaccountsegment') }}
            WHERE adh_active_flag = 1
        ) AS jas
        ON jas.id = a.journal_segment_id
    INNER JOIN
        (SELECT
            id,
            status_group_id
        FROM {{ source('structured_core_mm', 'eventtype') }} WHERE adh_active_flag = 1) AS et
        ON et.id = a.curr_status_sid
    INNER JOIN
        (SELECT
            id,
            name
        FROM {{ source('structured_core_mm', 'statusgroup') }} WHERE adh_active_flag = 1) AS sg
        ON sg.id = et.status_group_id
    WHERE
        a.commence_date
        <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
        AND coalesce(a.closed_date, '9999-12-31')
        >= '{{ var('Month_Start_Date','2022-04-01T00:00:00') }}'
        -- Considering only Account created before the Accrual period end date
        AND to_date(a.creation_date)
        <= '{{ var('UnbilledAccrualPeriodEndDate','2023-04-30T00:00:00') }}'
)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
