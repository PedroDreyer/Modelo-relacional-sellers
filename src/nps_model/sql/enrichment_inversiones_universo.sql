-- ============================================
-- Universo Total: Inversiones sobre base SEGMENTATION_SELLERS
-- ============================================
-- Base: SEGMENTATION_SELLERS (todos los sellers activos del producto)
-- Join: REMUNERADA_SELLERS para obtener distribucion de uso de inversiones
-- Parametros: {sites}, {fecha_minima_month}, {fecha_maxima_month}, {product_filter}

SELECT
    CAST(s.TIM_MONTH_TRANSACTION AS STRING) AS TIM_MONTH,
    COALESCE(i.FLAG_USA_INVERSIONES, 'No usa inversiones') AS FLAG_USA_INVERSIONES,
    CAST(COALESCE(i.FLAG_POTS_ACTIVO, 0) AS STRING) AS FLAG_POTS_ACTIVO,
    COUNT(DISTINCT s.CUS_CUST_ID) AS total_sellers
FROM `meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` s
LEFT JOIN `meli-bi-data.SBOX_NPS_ANALYTICS.REMUNERADA_SELLERS` i
    ON s.CUS_CUST_ID = i.CUS_CUST_ID
    AND CAST(s.TIM_MONTH_TRANSACTION AS STRING) = CAST(i.TIM_MONTH AS STRING)
WHERE s.SIT_SITE_ID IN {sites}
    AND s.TIM_MONTH_TRANSACTION >= {fecha_minima_month}
    AND s.TIM_MONTH_TRANSACTION <= {fecha_maxima_month}
    {product_filter}
GROUP BY 1, 2, 3
ORDER BY 1, 2
