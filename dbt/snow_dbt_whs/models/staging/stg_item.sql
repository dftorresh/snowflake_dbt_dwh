SELECT
    I_ITEM_SK AS ITEM_ID
    ,I_PRODUCT_NAME AS NAME
    ,I_CURRENT_PRICE CURRENT_PRICE
    ,I_WHOLESALE_COST WHOLESALE_COST
    ,CASE I_SIZE
        WHEN 'medium' THEN 'M'
        WHEN 'petite' THEN 'P'
        WHEN 'large' THEN 'L'
        WHEN 'economy' THEN 'EC'
        WHEN 'extra large' THEN 'XL'
        WHEN 'small' THEN 'S'
        WHEN 'N/A' THEN 'N/A'
        ELSE NULL
    END SIZE
    ,I_COLOR COLOR
    ,I_UNITS UNITS
FROM {{ ref('item_snapshot') }}