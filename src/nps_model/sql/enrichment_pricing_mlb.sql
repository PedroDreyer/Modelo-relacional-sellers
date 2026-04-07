-- ============================================
-- Enriquecimiento: Pricing por Escalas (MLB)
-- ============================================
-- Fuente: LK_POLITICAS_PRICING_DIARIO + LK_MP_PRODUCT_PRICING + LK_MP_PRODUCT_PRICING_SCALE
-- Campos output: PRICING (1/0), SCALE_LEVEL, PRICING_TYPE, PRICING_DESCRIPTION
-- Join key: CUS_CUST_ID + TIM_MONTH (mes del periodo de pricing)
-- Parámetros: {sites}, {fecha_minima}, {fecha_maxima}, {fecha_minima_month}, {fecha_maxima_month}

WITH pricing_raw AS (
    SELECT
        T.CUS_CUST_ID_SEL AS CUS_CUST_ID,
        CAST(T.PERIODO AS STRING) AS TIM_MONTH,
        T.ultima_politica,
        P.PRICING_TYPE,
        P.PRICING_DESCRIPTION,
        S.scale_level AS SCALE_LEVEL
    FROM `meli-bi-data.WHOWNER.LK_POLITICAS_PRICING_DIARIO` T
    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_PRODUCT_PRICING` P
        ON T.CUS_CUST_ID_SEL = P.CUS_CUST_ID
        AND P.SIT_SITE_ID IN {sites}
    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_PRODUCT_PRICING_SCALE` S
        ON P.pricing_detail_id = S.pricing_detail_id
    WHERE T.PERIODO >= {fecha_minima_month}
        AND T.PERIODO <= {fecha_maxima_month}
        -- Solo sellers que respondieron la encuesta NPS
        AND T.CUS_CUST_ID_SEL IN (
            SELECT DISTINCT NPS_TX_CUS_CUST_ID
            FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL`
            WHERE NPS_TX_END_DATE >= '{fecha_minima}'
                AND NPS_TX_END_DATE < '{fecha_maxima}'
                AND NPS_TX_SIT_SITE_ID IN {sites}
                AND NPS_TX_NOTA_NPS IS NOT NULL
        )
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY T.CUS_CUST_ID_SEL, T.PERIODO
        ORDER BY S.scale_level DESC NULLS LAST
    ) = 1
)

SELECT
    CUS_CUST_ID,
    TIM_MONTH,
    CASE WHEN SCALE_LEVEL IS NOT NULL THEN 1 ELSE 0 END AS PRICING,
    SCALE_LEVEL,
    PRICING_TYPE,
    PRICING_DESCRIPTION,
    ultima_politica
FROM pricing_raw
