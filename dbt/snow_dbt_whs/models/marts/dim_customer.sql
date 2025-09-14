{{ config(
    materialized='table',
    incremental_strategy='merge',
    unique_key='id'
) }}

SELECT
    CUSTOMER_ID AS CUSTOMER_SK
    ,FULL_NAME AS NAME
    ,COUNTRY AS ORIGIN_COUNTRY
    ,EMAIL
    ,PREFERRED_CUSTOMER
FROM {{ ref('stg_customer') }}