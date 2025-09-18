{{ config(
    materialized='incremental',
    unique_key = ['ITEM_ID', 'UPDATED_AT']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ITEM_ID', 'UPDATED_AT']) }} AS ITEM_SK
    ,ITEM_ID
    ,NAME
    ,CURRENT_PRICE
    ,WHOLESALE_COST
    ,SIZE
    ,COLOR
    ,UNITS
    ,UPDATED_AT
    ,VALID_FROM
    ,VALID_TO
FROM {{ ref('stg_item') }}
{% if is_incremental() %}
WHERE
    UPDATED_AT >= (SELECT MAX(UPDATED_AT) FROM {{ this }})
    OR VALID_TO >= (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}