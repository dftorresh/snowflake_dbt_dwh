{{config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='STORE_SK'
)}}

SELECT
    STORE_ID AS STORE_SK
    ,NAME
    ,MANAGER
    ,NUMBER_EMPLOYEES
    ,HOURS
    ,CITY
    ,COUNTY
    ,STATE
    ,STREET
FROM {{ ref('stg_store') }}