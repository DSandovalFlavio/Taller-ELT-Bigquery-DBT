SELECT
    DATE(DATE_ADD(oo.order_purchase_timestamp, INTERVAL 2125 DAY)) AS FCH_COMPRA,
    DATE_DIFF(
            DATE_ADD(oo.order_estimated_delivery_date, INTERVAL 2125 DAY),
            DATE_ADD(oo.order_purchase_timestamp, INTERVAL 2125 DAY),
            DAY) AS DIAS_ENTREGA_ESTIMADOS,
    DATE_DIFF(
            DATE_ADD(oo.order_delivered_customer_date, INTERVAL 2125 DAY),
            DATE_ADD(oo.order_purchase_timestamp, INTERVAL 2125 DAY),
            DAY) AS DIAS_ENTREGA_REALES,
    oo.order_status AS ORDEN_STATUS,
    oo.order_id AS ORDEN_ID,
    oo.customer_id AS CUSTOMER_ID,
    ooi.product_id AS PRODUCT_ID,
    COUNT(ooi.product_id) AS CANTIDAD_PRODUCTO,
    SUM(ooi.price) AS TOTAL_VENTA
FROM {{ source("bronze", "olist_orders_dataset") }} AS oo
INNER JOIN {{ source("bronze", "olist_order_items_dataset") }} AS ooi on oo.order_id = ooi.order_id
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 DESC