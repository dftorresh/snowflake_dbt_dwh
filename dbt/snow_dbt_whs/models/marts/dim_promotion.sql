{{
    config(
        materialized='incremental',
        unique_key=['PROMO_SK','UPDATED_AT']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['PROMO_ID','UPDATED_AT']) }} PROMO_SK
    ,PROMO_ID
    ,ITEM
    ,COST
    ,NAME
    ,DISCOUNT_ACTIVE
    ,UPDATED_AT
    ,VALID_FROM
    ,VALID_TO
FROM {{ ref('stg_promotion') }}
{% if is_incremental() %}
WHERE
    UPDATED_AT >= (SELECT MAX(UPDATED_AT) FROM {{ this }})
    -- So that the valid_to field gets updated in the target dim table.
    -- Because when the valid_to field is updated, the update_at value is not refreshed.
    OR VALID_TO >= (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}