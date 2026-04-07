-- ============================================
-- Universo Total: Distribución RANGO_LIMITE_TC + OFERTA_TC sobre SEGMENTATION_SELLERS
-- ============================================
-- Base: SEGMENTATION_SELLERS (todos los sellers activos del producto)
-- Join: BT_CCARD_CADASTRAL_20 para límite TC
--       LK_MP_MAUS_CREDIT_PROFILE para oferta TC
-- Parámetros: {sites}, {fecha_minima_month}, {fecha_maxima_month}, {product_filter}

WITH base AS (
    SELECT
        s.CUS_CUST_ID,
        CAST(s.TIM_MONTH_TRANSACTION AS STRING) AS TIM_MONTH,
        ROUND(tc.CCARD_LIMIT_GENERAL, 0) AS LIMITE_OFRECIDO,
        cp.FLAG_TC_OFFER
    FROM `meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` s
    LEFT JOIN `meli-bi-data.SBOX_CREDITSTC.BT_CCARD_CADASTRAL_20` tc
        ON s.CUS_CUST_ID = tc.CUS_CUST_ID
        AND s.TIM_MONTH_TRANSACTION = CAST(FORMAT_DATE('%Y%m', tc.TIM_MONTH) AS INT64)
        AND tc.TIM_MONTH >= PARSE_DATE('%Y%m', '{fecha_minima_month}')
        AND tc.TIM_MONTH <= PARSE_DATE('%Y%m', '{fecha_maxima_month}')
    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_MAUS_CREDIT_PROFILE` cp
        ON s.CUS_CUST_ID = cp.CUS_CUST_ID
        AND s.TIM_MONTH_TRANSACTION = cp.MONTH_ID
        AND cp.TIMEFRAME = 'END_OF_MONTH'
    WHERE s.SIT_SITE_ID IN {sites}
        AND s.TIM_MONTH_TRANSACTION >= {fecha_minima_month}
        AND s.TIM_MONTH_TRANSACTION <= {fecha_maxima_month}
        {product_filter}
)

SELECT
    TIM_MONTH,
    COALESCE(CAST(FLAG_TC_OFFER AS STRING), 'Sin dato') AS FLAG_TC_OFFER,
    {rango_case} AS RANGO_LIMITE_TC,
    COUNT(DISTINCT CUS_CUST_ID) AS total_sellers
FROM base
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
