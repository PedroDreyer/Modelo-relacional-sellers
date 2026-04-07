-- ============================================
-- Universo Total: Pricing sobre base SEGMENTATION_SELLERS (MLA / MLM)
-- ============================================
-- Parametros: {sites}, {fecha_minima_month}, {fecha_maxima_month}, {product_filter}

WITH escalas_dedup AS (
    SELECT
        P.CUS_CUST_ID,
        S.scale_level AS SCALE_LEVEL
    FROM `meli-bi-data.WHOWNER.LK_MP_PRODUCT_PRICING` P
    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_PRODUCT_PRICING_SCALE` S
        ON P.pricing_detail_id = S.pricing_detail_id
    WHERE P.PRICING_TYPE IN ('SCALE')
        AND P.PRICING_CREATE_DT > '2020-08-30'
        AND P.SIT_SITE_ID IN {sites}
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY P.CUS_CUST_ID
        ORDER BY S.PRICING_SCL_CREATE_DT DESC
    ) = 1
)

SELECT
    CAST(s.TIM_MONTH_TRANSACTION AS STRING) AS TIM_MONTH,
    CASE WHEN e.SCALE_LEVEL IS NOT NULL THEN 'Con pricing escalas' ELSE 'Sin pricing escalas' END AS FLAG_PRICING,
    CASE WHEN e.SCALE_LEVEL IS NOT NULL THEN CONCAT('Escala ', CAST(e.SCALE_LEVEL AS STRING)) ELSE 'Sin escala' END AS SCALE_LEVEL,
    COUNT(DISTINCT s.CUS_CUST_ID) AS total_sellers
FROM `meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` s
LEFT JOIN escalas_dedup e
    ON s.CUS_CUST_ID = e.CUS_CUST_ID
WHERE s.SIT_SITE_ID IN {sites}
    AND s.TIM_MONTH_TRANSACTION >= {fecha_minima_month}
    AND s.TIM_MONTH_TRANSACTION <= {fecha_maxima_month}
    {product_filter}
GROUP BY 1, 2, 3
ORDER BY 1, 2
