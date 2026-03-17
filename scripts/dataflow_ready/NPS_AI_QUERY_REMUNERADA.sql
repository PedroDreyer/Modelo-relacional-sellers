-- Job: NPS_AI_QUERY_REMUNERADA
-- Tabla: meli-bi-data.SBOX_NPS_ANALYTICS.REMUNERADA_SELLERS
-- Params: 2024-12-01 -> 2026-02-01 (YYYYMM: 202412 -> 202602)

CREATE OR REPLACE TABLE `meli-bi-data.SBOX_NPS_ANALYTICS.REMUNERADA_SELLERS` AS (
-- ============================================
-- Enriquecimiento: Inversiones (POTS) por Seller
-- ============================================
-- Trae flags de inversiones para cruzar con NPS
-- Join key: CUS_CUST_ID + TIM_MONTH
-- Parámetros: ('MLA','MLB','MLM','MLC','MLU','MCO','MPE'), 2024-12-01, 2026-02-01

SELECT
    a.CUS_CUST_ID,
    a.SIT_SITE_ID,
    CAST(FORMAT_DATE('%Y%m', a.TIM_DAY) AS STRING) AS TIM_MONTH,

    -- POTS
    MAX(CASE WHEN a.POTS_ACTIVE_QTY > 0 THEN 1 ELSE 0 END) AS FLAG_POTS_ACTIVO,
    MAX(a.POTS_ACTIVE_QTY) AS POTS_CANTIDAD,

    -- Inversiones (si están disponibles en la tabla)
    -- TODO: validar campos disponibles en DM_MP_INVESTMENTS
    -- MAX(a.INVESTED_BALANCE) AS SALDO_INVERTIDO,
    -- MAX(a.WINNER_FLAG) AS FLAG_WINNER,

    -- Flag consolidado
    CASE
        WHEN MAX(CASE WHEN a.POTS_ACTIVE_QTY > 0 THEN 1 ELSE 0 END) = 1
        THEN 'Usa inversiones'
        ELSE 'No usa inversiones'
    END AS FLAG_USA_INVERSIONES

FROM `meli-bi-data.WHOWNER.DM_MP_INVESTMENTS` a

WHERE
    a.TIM_DAY >= '2024-12-01'
    AND a.TIM_DAY < '2026-02-01'
    AND a.SIT_SITE_ID IN ('MLA','MLB','MLM','MLC','MLU','MCO','MPE')

GROUP BY 1, 2, 3

);
