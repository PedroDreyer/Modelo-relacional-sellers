-- ============================================
-- Universo Total: Credits sobre base SEGMENTATION_SELLERS
-- ============================================
-- Base: SEGMENTATION_SELLERS (todos los sellers activos del producto)
-- Join: CREDITS_SELLERS para obtener distribución FRED
-- Parámetros: {sites}, {fecha_minima_month}, {fecha_maxima_month}, {product_filter}

SELECT
    CAST(s.TIM_MONTH_TRANSACTION AS STRING) AS TIM_MONTH,
    COALESCE(c.CREDIT_GROUP, 'Sin dato credits') AS CREDIT_GROUP,
    COALESCE(c.FLAG_USA_CREDITO, 'Sin dato') AS FLAG_USA_CREDITO,
    COALESCE(c.FLAG_TARJETA_CREDITO, 'Sin dato') AS FLAG_TARJETA_CREDITO,
    COALESCE(c.ESTADO_OFERTA_CREDITO, 'Sin dato') AS ESTADO_OFERTA_CREDITO,
    COUNT(DISTINCT s.CUS_CUST_ID) AS total_sellers
FROM `meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` s
LEFT JOIN `meli-bi-data.SBOX_NPS_ANALYTICS.CREDITS_SELLERS` c
    ON s.CUS_CUST_ID = c.CUS_CUST_ID
    AND s.TIM_MONTH_TRANSACTION = c.TIM_MONTH
WHERE s.SIT_SITE_ID IN {sites}
    AND s.TIM_MONTH_TRANSACTION >= {fecha_minima_month}
    AND s.TIM_MONTH_TRANSACTION <= {fecha_maxima_month}
    {product_filter}
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2
