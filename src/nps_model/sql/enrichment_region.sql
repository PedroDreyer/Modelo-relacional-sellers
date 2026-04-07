-- ============================================
-- Enrichment: Region/Provincia del Seller
-- ============================================
-- Fuente: LK_MP_SELLERS_STORES_GEO_NEW (ubicación del store)
-- Solo habilitado para MLA (variaciones por IIBB provincial)
-- Join key: CUS_CUST_ID
-- Campos: REGION (provincia), CIUDAD
-- Parametros: {sites}, {fecha_minima}, {fecha_maxima}

SELECT DISTINCT
    r.NPS_TX_CUS_CUST_ID AS CUS_CUST_ID,
    CAST(FORMAT_DATETIME('%Y%m', r.NPS_TX_END_DATE) AS STRING) AS TIM_MONTH,
    COALESCE(g.GEO_STATE, 'Sin dato') AS REGION,
    COALESCE(g.GEO_CITY, 'Sin dato') AS CIUDAD
FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL` r
LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_SELLERS_STORES_GEO_NEW` g
    ON g.CUS_CUST_ID = r.NPS_TX_CUS_CUST_ID
WHERE r.NPS_TX_END_DATE >= '{fecha_minima}'
    AND r.NPS_TX_END_DATE < '{fecha_maxima}'
    AND r.NPS_TX_SIT_SITE_ID IN {sites}
    AND r.NPS_TX_NOTA_NPS IS NOT NULL
