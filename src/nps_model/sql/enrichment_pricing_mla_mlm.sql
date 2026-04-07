-- ============================================
-- Enriquecimiento: Pricing por Escalas (MLA / MLM)
-- ============================================
-- Fuente: LK_MP_PRODUCT_PRICING + LK_MP_PRODUCT_PRICING_SCALE
-- Campos output: PRICING (1/0), SCALE_LEVEL, PRICING_TYPE, PRICING_DESCRIPTION
-- Join key: CUS_CUST_ID (se toma la última escala vigente al momento de la encuesta)
-- Parámetros: {sites}, {fecha_minima}, {fecha_maxima}

WITH escalas_raw AS (
    SELECT
        P.CUS_CUST_ID,
        P.PRICING_TYPE,
        P.PRICING_DESCRIPTION,
        S.scale_level AS SCALE_LEVEL,
        S.PRICING_SCL_CREATE_DT,
        CAST(FORMAT_DATETIME("%Y%m", S.PRICING_SCL_CREATE_DT) AS INT64) AS TIM_MONTH_ESCALA
    FROM `meli-bi-data.WHOWNER.LK_MP_PRODUCT_PRICING` P
    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_PRODUCT_PRICING_SCALE` S
        ON P.pricing_detail_id = S.pricing_detail_id
    WHERE P.PRICING_TYPE IN ('SCALE')
        AND P.PRICING_CREATE_DT > '2020-08-30'
        AND P.SIT_SITE_ID IN {sites}
        -- Solo sellers que respondieron la encuesta NPS
        AND P.CUS_CUST_ID IN (
            SELECT DISTINCT NPS_TX_CUS_CUST_ID
            FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL`
            WHERE NPS_TX_END_DATE >= '{fecha_minima}'
                AND NPS_TX_END_DATE < '{fecha_maxima}'
                AND NPS_TX_SIT_SITE_ID IN {sites}
                AND NPS_TX_NOTA_NPS IS NOT NULL
        )
),

-- Para cada seller, tomar la última escala vigente
escalas_dedup AS (
    SELECT
        CUS_CUST_ID,
        PRICING_TYPE,
        PRICING_DESCRIPTION,
        SCALE_LEVEL,
        PRICING_SCL_CREATE_DT
    FROM escalas_raw
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY CUS_CUST_ID
        ORDER BY PRICING_SCL_CREATE_DT DESC
    ) = 1
),

-- Cruzar con encuestas NPS para obtener TIM_MONTH del seller
nps_sellers AS (
    SELECT DISTINCT
        NPS_TX_CUS_CUST_ID AS CUS_CUST_ID,
        CAST(FORMAT_DATETIME("%Y%m", NPS_TX_END_DATE) AS STRING) AS TIM_MONTH
    FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL`
    WHERE NPS_TX_END_DATE >= '{fecha_minima}'
        AND NPS_TX_END_DATE < '{fecha_maxima}'
        AND NPS_TX_SIT_SITE_ID IN {sites}
        AND NPS_TX_NOTA_NPS IS NOT NULL
)

SELECT
    n.CUS_CUST_ID,
    n.TIM_MONTH,
    CASE WHEN e.SCALE_LEVEL IS NOT NULL THEN 1 ELSE 0 END AS PRICING,
    e.SCALE_LEVEL,
    e.PRICING_TYPE,
    e.PRICING_DESCRIPTION
FROM nps_sellers n
LEFT JOIN escalas_dedup e ON n.CUS_CUST_ID = e.CUS_CUST_ID
