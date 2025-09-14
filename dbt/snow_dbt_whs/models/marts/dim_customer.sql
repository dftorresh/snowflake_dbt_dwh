{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='CUSTOMER_SK'
) }}

SELECT
    CUSTOMER_ID AS CUSTOMER_SK
    ,FULL_NAME AS NAME
    ,COUNTRY AS ORIGIN_COUNTRY
    ,EMAIL
    ,PREFERRED_CUSTOMER
FROM {{ ref('stg_customer') }}