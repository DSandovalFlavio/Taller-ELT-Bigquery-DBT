SELECT 
    deo.FCH_COMPRA, deo.DIAS_ENTREGA_ESTIMADOS, deo.DIAS_ENTREGA_REALES, 
    deo.ORDEN_STATUS, deo.ORDEN_ID, deo.CUSTOMER_ID, deo.PRODUCT_ID,
    pyd.NUM_PAGOS,
    cd.CUSTOMER_ZIP_CODE_PREFIX, cd.CUSTOMER_CITY, cd.CUSTOMER_STATE,
    deo.CANTIDAD_PRODUCTO, deo.TOTAL_VENTA
FROM {{ ref("det_ent_ord") }} as deo
LEFT JOIN {{ ref("pyments_det") }} as pyd
    ON deo.ORDEN_ID = pyd.ORDEN_ID
LEFT JOIN {{ ref("cust_detail") }} as cd
    ON deo.CUSTOMER_ID = cd.CUSTOMER_ID