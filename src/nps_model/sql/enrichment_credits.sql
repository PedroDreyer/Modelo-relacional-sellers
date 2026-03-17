-- ============================================
-- Enriquecimiento: Créditos por Seller
-- ============================================
-- Fuente: SBOX_NPS_ANALYTICS.CREDITS_SELLERS (tabla Dataflow)
-- Campos: CREDIT_GROUP, FLAG_USA_CREDITO, FLAG_TARJETA_CREDITO, ESTADO_OFERTA_CREDITO
-- Join key: CUS_CUST_ID + TIM_MONTH
-- Parámetros: {sites}, {fecha_minima}, {fecha_maxima}, {fecha_minima_month}, {fecha_maxima_month}

SELECT
    f.CUS_CUST_ID,
    CAST(f.TIM_MONTH AS STRING) AS TIM_MONTH,
    f.CREDIT_GROUP,
    f.FLAG_OFFER_CREDITS,
    f.FLAG_OFFER_MCREDITS,
    f.FLAG_TC_OFFER,
    f.FLAG_ROW_PERSONAL_LOAN,
    f.USO_MERCHANT_CREDIT,
    f.USO_SECURED_LOAN,
    f.USO_CONSUMER_CREDIT,
    f.USO_PERSONAL_LOAN,
    f.USO_TARJETA_CREDITO,
    f.FLAG_USA_CREDITO,
    f.FLAG_TARJETA_CREDITO,
    f.ESTADO_OFERTA_CREDITO

FROM `meli-bi-data.SBOX_NPS_ANALYTICS.CREDITS_SELLERS` f

WHERE f.TIM_MONTH >= {fecha_minima_month}
    AND f.TIM_MONTH <= {fecha_maxima_month}
    -- Solo sellers que respondieron la encuesta NPS (evita OOM)
    AND f.CUS_CUST_ID IN (
        SELECT DISTINCT NPS_TX_CUS_CUST_ID
        FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL`
        WHERE NPS_TX_END_DATE >= '{fecha_minima}'
            AND NPS_TX_END_DATE < '{fecha_maxima}'
            AND NPS_TX_SIT_SITE_ID IN {sites}
            AND NPS_TX_NOTA_NPS IS NOT NULL
    )
