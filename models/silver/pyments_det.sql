SELECT order_id AS ORDEN_ID ,
MAX(payment_sequential) AS NUM_PAGOS
FROM {{ source("bronze", "olist_order_payments_dataset") }} 
GROUP BY 1