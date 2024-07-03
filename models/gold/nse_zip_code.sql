WITH percentiles AS (
  SELECT
    CUSTOMER_ZIP_CODE_PREFIX,
    PERCENTILE_CONT(TOTAL_VENTA, 0.2) OVER () AS p20,
    PERCENTILE_CONT(TOTAL_VENTA, 0.4) OVER () AS p40,
    PERCENTILE_CONT(TOTAL_VENTA, 0.6) OVER () AS p60,
    PERCENTILE_CONT(TOTAL_VENTA, 0.8) OVER () AS p80,
    PERCENTILE_CONT(TOTAL_VENTA, 0.99) OVER () AS p99
  FROM (
    SELECT
      cd.CUSTOMER_ZIP_CODE_PREFIX,
      SUM(deo.TOTAL_VENTA) AS TOTAL_VENTA
    FROM {{ ref("det_ent_ord") }} as deo
    LEFT JOIN {{ ref("cust_detail") }} as cd
        ON deo.CUSTOMER_ID = cd.CUSTOMER_ID
    GROUP BY 1
    ORDER BY 2
  ) AS ventas_totales
)
SELECT
  p.CUSTOMER_ZIP_CODE_PREFIX,
  ROUND(SUM(deo.TOTAL_VENTA), 2) AS TOTAL_VENTA,  -- Round here for final output
  CASE
    WHEN TOTAL_VENTA <= ROUND(p.p20, 2) THEN 'NSE Bajo'
    WHEN TOTAL_VENTA > ROUND(p.p20, 2) AND TOTAL_VENTA <= ROUND(p.p40, 2) THEN 'NSE Medio Bajo'
    WHEN TOTAL_VENTA > ROUND(p.p40, 2) AND TOTAL_VENTA <= ROUND(p.p60, 2) THEN 'NSE Medio'
    WHEN TOTAL_VENTA > ROUND(p.p60, 2) AND TOTAL_VENTA <= ROUND(p.p80, 2) THEN 'NSE Medio Alto'
    WHEN TOTAL_VENTA > ROUND(p.p80, 2) AND TOTAL_VENTA <= ROUND(p.p99, 2) THEN 'NSE Alto'
    ELSE 'NSE Muy Alto'
  END AS NSE
FROM {{ ref("det_ent_ord") }} as deo
LEFT JOIN {{ ref("cust_detail") }} as cd
    ON deo.CUSTOMER_ID = cd.CUSTOMER_ID
LEFT JOIN percentiles AS p
    ON cd.CUSTOMER_ZIP_CODE_PREFIX = p.CUSTOMER_ZIP_CODE_PREFIX
GROUP BY 1, 3