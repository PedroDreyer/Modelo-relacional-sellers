-- ============================================
-- Universo Total: Top Off sobre base SEGMENTATION_SELLERS
-- ============================================
-- Base: SEGMENTATION_SELLERS (todos los sellers activos del producto)
-- Join: BT_CX_SELLERS_MP_TOP_OFF para flag topoff
-- Parámetros: {sites}, {fecha_minima_month}, {fecha_maxima_month}, {product_filter}

SELECT
    CAST(s.TIM_MONTH_TRANSACTION AS STRING) AS TIM_MONTH,
    CASE WHEN t.CUS_CUST_ID IS NOT NULL THEN 'Con Top Off' ELSE 'Sin Top Off' END AS FLAG_TOPOFF,
    COUNT(DISTINCT s.CUS_CUST_ID) AS total_sellers
FROM `meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` s
LEFT JOIN `meli-bi-data.WHOWNER.BT_CX_SELLERS_MP_TOP_OFF` t
    ON s.CUS_CUST_ID = t.CUS_CUST_ID
    AND s.SIT_SITE_ID = t.SIT_SITE_ID
WHERE s.SIT_SITE_ID IN {sites}
    AND s.TIM_MONTH_TRANSACTION >= {fecha_minima_month}
    AND s.TIM_MONTH_TRANSACTION <= {fecha_maxima_month}
    {product_filter}
GROUP BY 1, 2
ORDER BY 1, 2
