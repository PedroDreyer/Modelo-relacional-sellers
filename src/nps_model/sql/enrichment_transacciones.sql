-- ============================================
-- Enriquecimiento: Transacciones (TPV/TPN) por Seller
-- ============================================
-- Trae volumen transaccional y segmentación para cruzar con NPS
-- Join key: CUS_CUST_ID + TIM_MONTH
-- Parámetros: {sites}, {fecha_minima}, {fecha_maxima}

WITH transacciones_sellers AS (
    SELECT
        m.CUS_CUST_ID,
        m.SIT_SITE_ID,
        CAST(FORMAT_DATETIME("%Y%m", m.TIM_DAY) AS STRING) AS TIM_MONTH,

        -- TPN por producto
        ROUND(SUM(m.TPN_POINT), 0) AS TPN_POINT,
        ROUND(SUM(m.TPN_QR), 0) AS TPN_QR,
        ROUND(SUM(m.TPN_OP_LINK), 0) AS TPN_LINK,
        ROUND(SUM(m.TPN_OP_COW_API), 0) AS TPN_API,
        ROUND(SUM(m.TPN_TRS), 0) AS TPN_TRANSFERENCIAS,

        -- TPV por producto (en dólares)
        ROUND(SUM(m.TPV_POINT_DOL), 0) AS TPV_POINT,
        ROUND(SUM(m.TPV_QR_DOL), 0) AS TPV_QR,
        ROUND(SUM(m.TPV_OP_LINK_DOL), 0) AS TPV_LINK,
        ROUND(SUM(m.TPV_OP_COW_API_DOL), 0) AS TPV_API,

        -- Totales
        ROUND(SUM(COALESCE(m.TPN_POINT, 0) + COALESCE(m.TPN_QR, 0)
              + COALESCE(m.TPN_OP_LINK, 0) + COALESCE(m.TPN_OP_COW_API, 0)
              + COALESCE(m.TPN_TRS, 0)), 0) AS TPN_TOTAL,
        ROUND(SUM(COALESCE(m.TPV_POINT_DOL, 0) + COALESCE(m.TPV_QR_DOL, 0)
              + COALESCE(m.TPV_OP_LINK_DOL, 0) + COALESCE(m.TPV_OP_COW_API_DOL, 0)), 0) AS TPV_TOTAL

    FROM `meli-bi-data.WHOWNER.LK_MP_MASTER_SELLERS` m
    WHERE
        m.TIM_DAY >= '{fecha_minima}'
        AND m.TIM_DAY < '{fecha_maxima}'
        AND m.SIT_SITE_ID IN {sites}
    GROUP BY 1, 2, 3
),

-- PF/PJ desde KYC
kyc AS (
    SELECT
        CUS_CUST_ID,
        CASE
            WHEN KYC_ENTITY_TYPE = 'person' THEN 'PF'
            WHEN KYC_ENTITY_TYPE = 'company' THEN 'PJ'
            ELSE 'Otro'
        END AS TIPO_PERSONA_KYC
    FROM `meli-bi-data.WHOWNER.LK_KYC_VAULT_USER`
)

SELECT
    t.*,
    k.TIPO_PERSONA_KYC,

    -- Rangos de TPN total
    CASE
        WHEN t.TPN_TOTAL = 0 THEN '0 txn'
        WHEN t.TPN_TOTAL BETWEEN 1 AND 10 THEN '1-10 txn'
        WHEN t.TPN_TOTAL BETWEEN 11 AND 50 THEN '11-50 txn'
        WHEN t.TPN_TOTAL BETWEEN 51 AND 200 THEN '51-200 txn'
        WHEN t.TPN_TOTAL BETWEEN 201 AND 1000 THEN '201-1000 txn'
        ELSE '1000+ txn'
    END AS RANGO_TPN,

    -- Rangos de TPV total (USD)
    CASE
        WHEN t.TPV_TOTAL = 0 THEN '$0'
        WHEN t.TPV_TOTAL BETWEEN 1 AND 100 THEN '$1-100'
        WHEN t.TPV_TOTAL BETWEEN 101 AND 500 THEN '$101-500'
        WHEN t.TPV_TOTAL BETWEEN 501 AND 2000 THEN '$501-2K'
        WHEN t.TPV_TOTAL BETWEEN 2001 AND 10000 THEN '$2K-10K'
        ELSE '$10K+'
    END AS RANGO_TPV

FROM transacciones_sellers t
LEFT JOIN kyc k ON t.CUS_CUST_ID = k.CUS_CUST_ID
