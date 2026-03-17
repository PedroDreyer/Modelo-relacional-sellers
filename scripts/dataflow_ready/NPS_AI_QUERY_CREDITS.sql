-- Job: NPS_AI_QUERY_CREDITS
-- Tabla: meli-bi-data.SBOX_NPS_ANALYTICS.CREDITS_SELLERS
-- Params: 2024-12-01 -> 2026-02-01 (YYYYMM: 202412 -> 202602)

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_NPS_ANALYTICS.CREDITS_SELLERS` AS (
-- ============================================
-- Enriquecimiento: Créditos e Inversiones por Seller
-- ============================================
-- Trae perfil crediticio por seller para cruzar con NPS
-- Join key: CUS_CUST_ID + TIM_MONTH
-- Parámetros: ('MLA','MLB','MLM','MLC','MLU','MCO','MPE'), 202412, 202602

SELECT
    f.CUS_CUST_ID,
    f.TIM_MONTH,

    -- Grupo y ofertas
    f.CREDIT_GROUP,
    f.FLAG_OFFER_CREDITS,
    f.FLAG_OFFER_MCREDITS,
    f.FLAG_TC_OFFER,
    f.FLAG_ROW_PERSONAL_LOAN,

    -- Uso de productos financieros
    f.PRODUCT_USED.MERCHANT_CREDIT AS USO_MERCHANT_CREDIT,
    f.PRODUCT_USED.SECURED_LOAN AS USO_SECURED_LOAN,
    f.PRODUCT_USED.CONSUMER_CREDIT AS USO_CONSUMER_CREDIT,
    f.PRODUCT_USED.PERSONAL_LOAN AS USO_PERSONAL_LOAN,
    f.PRODUCT_USED.CREDIT_CARD AS USO_TARJETA_CREDITO,

    -- Flags derivados para análisis
    CASE
        WHEN COALESCE(f.PRODUCT_USED.MERCHANT_CREDIT, 0) > 0
          OR COALESCE(f.PRODUCT_USED.CONSUMER_CREDIT, 0) > 0
          OR COALESCE(f.PRODUCT_USED.PERSONAL_LOAN, 0) > 0
          OR COALESCE(f.PRODUCT_USED.SECURED_LOAN, 0) > 0
        THEN 'Usa crédito'
        ELSE 'No usa crédito'
    END AS FLAG_USA_CREDITO,

    CASE
        WHEN COALESCE(f.PRODUCT_USED.CREDIT_CARD, 0) > 0
        THEN 'Tiene TC MP'
        ELSE 'Sin TC MP'
    END AS FLAG_TARJETA_CREDITO,

    CASE
        WHEN f.FLAG_OFFER_CREDITS = 1 AND COALESCE(f.PRODUCT_USED.MERCHANT_CREDIT, 0) = 0
             AND COALESCE(f.PRODUCT_USED.PERSONAL_LOAN, 0) = 0
        THEN 'Oferta no tomada'
        WHEN f.FLAG_OFFER_CREDITS = 1 THEN 'Oferta tomada'
        ELSE 'Sin oferta'
    END AS ESTADO_OFERTA_CREDITO

FROM `meli-bi-data.WHOWNER.LK_MP_MAUS_CREDIT_PROFILE` f

WHERE
    CAST(f.TIM_MONTH AS STRING) >= '202412'
    AND CAST(f.TIM_MONTH AS STRING) <= '202602'

);
