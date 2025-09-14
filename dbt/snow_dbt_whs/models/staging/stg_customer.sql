SELECT
    TOP 10
    C_CUSTOMER_SK AS CUSTOMER_ID
    ,TRIM(
        REPLACE(
            IFNULL(C_SALUTATION,'') || ' ' ||
            IFNULL(C_FIRST_NAME,'') || ' ' ||
            IFNULL(C_LAST_NAME,'')
        ,'  ','')
    ) AS FULL_NAME
    ,C_BIRTH_COUNTRY AS COUNTRY
    ,C_EMAIL_ADDRESS AS EMAIL
    ,C_PREFERRED_CUST_FLAG AS PREFERRED_CUSTOMER
FROM {{ source('raw', 'CUSTOMER') }}