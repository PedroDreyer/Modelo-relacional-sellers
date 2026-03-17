-- ============================================
-- Enriquecimiento: Atención al cliente (Top Off)
-- Fuente: meli-bi-data.WHOWNER.BT_CX_SELLERS_MP_TOP_OFF
-- Granularidad: 1 fila por CUS_CUST_ID + SIT_SITE_ID (sin mes, es tabla de estado)
-- Join key: CUS_CUST_ID + SIT_SITE_ID (sin TIM_MONTH)
-- Parámetros: {sites}
-- ============================================

SELECT DISTINCT
    CUS_CUST_ID,
    SIT_SITE_ID,
    CATEGORY AS TOPOFF_CATEGORY,
    1 AS FLAG_TOPOFF
FROM `meli-bi-data.WHOWNER.BT_CX_SELLERS_MP_TOP_OFF`
WHERE SIT_SITE_ID IN {sites}
