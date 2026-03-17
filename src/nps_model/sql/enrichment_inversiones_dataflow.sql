-- ============================================
-- Inversiones desde tabla Dataflow REMUNERADA_SELLERS
-- Filtrado por NPS sellers para evitar OOM (tabla tiene 1.96B rows)
-- Parámetros: {sites}, {fecha_minima}, {fecha_maxima}, {fecha_minima_month}, {fecha_maxima_month}
-- ============================================

SELECT
    r.TIM_MONTH,
    r.CUS_CUST_ID,
    r.SIT_SITE_ID,
    r.FLAG_INVERSIONES,
    r.FLAG_ASSET,
    r.FLAG_POTS_ACTIVO,
    r.FLAG_WINNER,
    r.FLAG_USA_INVERSIONES
FROM `meli-bi-data.SBOX_NPS_ANALYTICS.REMUNERADA_SELLERS` r
WHERE CAST(r.TIM_MONTH AS INT64) >= {fecha_minima_month}
    AND CAST(r.TIM_MONTH AS INT64) <= {fecha_maxima_month}
    AND r.SIT_SITE_ID IN {sites}
    AND r.CUS_CUST_ID IN (
        SELECT DISTINCT NPS_TX_CUS_CUST_ID
        FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL`
        WHERE NPS_TX_END_DATE >= '{fecha_minima}'
            AND NPS_TX_END_DATE < '{fecha_maxima}'
            AND NPS_TX_SIT_SITE_ID IN {sites}
            AND NPS_TX_NOTA_NPS IS NOT NULL
    )
