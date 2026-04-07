-- ============================================
-- Universo Total: Pricing sobre base SEGMENTATION_SELLERS (MLB)
-- ============================================
-- Parametros: {sites}, {fecha_minima_month}, {fecha_maxima_month}, {product_filter}

WITH pricing_dedup AS (
    SELECT
        T.CUS_CUST_ID_SEL AS CUS_CUST_ID,
        CAST(T.PERIODO AS STRING) AS TIM_MONTH,
        S.scale_level AS SCALE_LEVEL
    FROM `meli-bi-data.WHOWNER.LK_POLITICAS_PRICING_DIARIO` T
    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_PRODUCT_PRICING` P
        ON T.CUS_CUST_ID_SEL = P.CUS_CUST_ID
        AND P.SIT_SITE_ID IN {sites}
    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_PRODUCT_PRICING_SCALE` S
        ON P.pricing_detail_id = S.pricing_detail_id
    WHERE T.PERIODO >= {fecha_minima_month}
        AND T.PERIODO <= {fecha_maxima_month}
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY T.CUS_CUST_ID_SEL, T.PERIODO
        ORDER BY S.scale_level DESC NULLS LAST
    ) = 1
)

SELECT
    CAST(s.TIM_MONTH_TRANSACTION AS STRING) AS TIM_MONTH,
    CASE WHEN p.SCALE_LEVEL IS NOT NULL THEN 'Con pricing escalas' ELSE 'Sin pricing escalas' END AS FLAG_PRICING,
    CASE WHEN p.SCALE_LEVEL IS NOT NULL THEN CONCAT('Escala ', CAST(p.SCALE_LEVEL AS STRING)) ELSE 'Sin escala' END AS SCALE_LEVEL,
    COUNT(DISTINCT s.CUS_CUST_ID) AS total_sellers
FROM `meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` s
LEFT JOIN pricing_dedup p
    ON s.CUS_CUST_ID = p.CUS_CUST_ID
    AND CAST(s.TIM_MONTH_TRANSACTION AS STRING) = p.TIM_MONTH
WHERE s.SIT_SITE_ID IN {sites}
    AND s.TIM_MONTH_TRANSACTION >= {fecha_minima_month}
    AND s.TIM_MONTH_TRANSACTION <= {fecha_maxima_month}
    {product_filter}
GROUP BY 1, 2, 3
ORDER BY 1, 2
