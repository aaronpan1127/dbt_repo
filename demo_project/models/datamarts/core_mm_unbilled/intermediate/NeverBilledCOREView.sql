/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='ephemeral') }}


WITH source_data AS (

    SELECT DISTINCT
        a.account_id AS seq_product_item_id,
        a.service_sdt,
        a.service_edt,
        a.fuel_type,
        a.market_region,
        a.market_sub_region,
        a.customer_type AS customer_segment,
        date_part('MONTH', a.service_sdt) AS mth_no
    FROM {{ ref('active_accounts_view') }} AS a
    LEFT ANTI JOIN {{ ref('RegistryCore') }} AS rc ON rc.seq_product_item_id = a.account_id
    WHERE a.service_sdt <= a.service_edt

)

SELECT *
FROM source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
