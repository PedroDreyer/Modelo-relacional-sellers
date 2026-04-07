-- ============================================
-- Enriquecimiento: Límite TC + Oferta TC por Seller
-- ============================================
-- Fuente: SBOX_CREDITSTC.BT_CCARD_CADASTRAL_20 (límite TC)
--         WHOWNER.LK_MP_MAUS_CREDIT_PROFILE (oferta TC)
-- TIM_MONTH en BT_CCARD_CADASTRAL_20 es DATE (primer día del mes)
-- MONTH_ID en LK_MP_MAUS_CREDIT_PROFILE es DATE
-- Parámetros: {sites}, {fecha_minima}, {fecha_maxima}, {fecha_minima_month}, {fecha_maxima_month}

SELECT
    tc.CUS_CUST_ID,
    FORMAT_DATE('%Y%m', tc.TIM_MONTH) AS TIM_MONTH,
    ROUND(tc.CCARD_LIMIT_GENERAL, 0) AS LIMITE_OFRECIDO,
    ROUND(tc.CCARD_LIMIT_USED_AMOUNT_LC, 0) AS LIMITE_CONSUMIDO,
    cp.FLAG_TC_OFFER

FROM `meli-bi-data.SBOX_CREDITSTC.BT_CCARD_CADASTRAL_20` tc
LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_MAUS_CREDIT_PROFILE` cp
    ON tc.CUS_CUST_ID = cp.CUS_CUST_ID
    AND CAST(FORMAT_DATE('%Y%m', tc.TIM_MONTH) AS INT64) = cp.MONTH_ID
    AND cp.TIMEFRAME = 'END_OF_MONTH'

WHERE tc.TIM_MONTH >= PARSE_DATE('%Y%m', '{fecha_minima_month}')
    AND tc.TIM_MONTH <= PARSE_DATE('%Y%m', '{fecha_maxima_month}')
    AND tc.CUS_CUST_ID IN (
        SELECT DISTINCT NPS_TX_CUS_CUST_ID
        FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL`
        WHERE NPS_TX_END_DATE >= '{fecha_minima}'
            AND NPS_TX_END_DATE < '{fecha_maxima}'
            AND NPS_TX_SIT_SITE_ID IN {sites}
            AND NPS_TX_NOTA_NPS IS NOT NULL
    )
