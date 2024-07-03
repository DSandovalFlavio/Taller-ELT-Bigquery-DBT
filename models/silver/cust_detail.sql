SELECT DISTINCT
    oc.customer_id AS CUSTOMER_ID,
    oc.customer_zip_code_prefix AS CUSTOMER_ZIP_CODE_PREFIX,
    oc.customer_city AS CUSTOMER_CITY,
    oc.customer_state AS CUSTOMER_STATE,
FROM {{ source("bronze", "olist_customers_dataset") }}  AS oc