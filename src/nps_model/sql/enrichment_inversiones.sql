-- ============================================
-- Tabla: flags_inversiones_por_mes
-- Descripción: CTE con flags de inversión por seller y mes.
-- FLAG_INVERSIONES: algún producto activo entre LCI_LCA, CDB, POTS, FUNDS, CRYPTO.
-- FLAG_ASSET: cuenta remunerada (ASSET) activa.
-- FLAG_POTS: POTS activo (POTS_ACTIVE_QTY > 0).
-- FLAG_WINNER: 1 si tuvo rendimiento extra en algún momento del mes Y usó ASSET o POTS.
-- Fuente: meli-bi-data.WHOWNER.DM_MP_INVESTMENTS_BY_PRODUCT
--         meli-bi-data.SBOX_INVESTMENTSMLB.BT_MP_ORACULUM_CONTA_DIARIO_MASTER (MLB)
--         meli-bi-data.WHOWNER.LK_MP_GAMIFICATION_SETTINGS (MLM)
--         meli-bi-data.WHOWNER.LK_MP_GAMIFICATION_CHALLENGE (MLM)
-- Granularidad: 1 fila por CUS_CUST_ID + TIM_MONTH + SIT_SITE_ID
-- Nota: FLAG_WINNER aplica solo en MLB y MLM.
--       MLB usa IS_GAME=true (tabla Oraculum diaria) para rendimiento extra.
--       MLM usa gamification con carry-over al mes siguiente de la victoria.
--       Para otros sites (MLA, MLC, etc.) FLAG_WINNER queda en 0.
-- Parámetros: {sites}, {fecha_minima_month}, {fecha_maxima_month}
-- ============================================

WITH
-- === MLB: Sellers con rendimiento extra desde tabla Oraculum diaria ===
winners_mlb AS (
    SELECT
        O.CUS_CUST_ID,
        CAST(FORMAT_DATE('%Y%m', O.TIM_MONTH) AS INT64) AS TIM_MONTH
    FROM `meli-bi-data.SBOX_INVESTMENTSMLB.BT_MP_ORACULUM_CONTA_DIARIO_MASTER` O
    WHERE O.IS_GAME = true
    GROUP BY 1, 2
),

-- === MLM: Sellers que ganaron challenge PLUS_ACCOUNT (base cruda) ===
winners_mlm_raw AS (
    SELECT DISTINCT
        S.CUS_CUST_ID,
        DATE_TRUNC(DATE_SUB(DATE(C.PLAY_TIME_TO_DT), INTERVAL 1 DAY), MONTH) AS WIN_MONTH_DATE
    FROM `meli-bi-data.WHOWNER.LK_MP_GAMIFICATION_SETTINGS` S
    JOIN `meli-bi-data.WHOWNER.LK_MP_GAMIFICATION_CHALLENGE` C
        ON C.CHALLENGE_ID = S.CHALLENGE_ID
    WHERE S.GAME_RESULT = 'WON'
        AND S.SIT_SITE_ID = 'MLM'
        AND (C.RECURRENT_TYPE = 'PLUS_ACCOUNT' OR C.AWARD_TYPE = 'PLUS_ACCOUNT')
),

-- === MLM: Expande al mes de victoria + mes siguiente (carry-over), dedup ===
winners_mlm AS (
    SELECT DISTINCT
        CUS_CUST_ID,
        CAST(FORMAT_DATE('%Y%m', WIN_MONTH_DATE) AS INT64) AS TIM_MONTH
    FROM winners_mlm_raw

    UNION DISTINCT

    SELECT DISTINCT
        CUS_CUST_ID,
        CAST(FORMAT_DATE('%Y%m', DATE_ADD(WIN_MONTH_DATE, INTERVAL 1 MONTH)) AS INT64) AS TIM_MONTH
    FROM winners_mlm_raw
),

-- === Unión de winners de todos los sites ===
winners_all AS (
    SELECT CUS_CUST_ID, 'MLB' AS SIT_SITE_ID, TIM_MONTH FROM winners_mlb
    UNION ALL
    SELECT CUS_CUST_ID, 'MLM' AS SIT_SITE_ID, TIM_MONTH FROM winners_mlm
),

-- === CTE principal: flags de inversión por seller/mes/site ===
flags_inversiones_por_mes AS (
    SELECT
        I.TIM_MONTH_ID AS TIM_MONTH,
        I.CUS_CUST_ID,
        I.SIT_SITE_ID,
        MAX(CASE WHEN PRODUCT IN ('LCI_LCA', 'CDB', 'POTS', 'FUNDS', 'CRYPTO')
            AND (HOLD_AMT_USD > 0 OR HOLD_QTY > 0) THEN 1 ELSE 0 END) AS FLAG_INVERSIONES,
        MAX(CASE WHEN PRODUCT = 'ASSET'
            AND (HOLD_AMT_USD > 0 OR HOLD_QTY > 0) THEN 1 ELSE 0 END) AS FLAG_ASSET,
        MAX(CASE WHEN PRODUCT = 'POTS'
            AND POTS_ACTIVE_QTY > 0 THEN 1 ELSE 0 END) AS FLAG_POTS,
        MAX(CASE WHEN (
            (PRODUCT = 'ASSET' AND (HOLD_AMT_USD > 0 OR HOLD_QTY > 0))
            OR (PRODUCT = 'POTS' AND POTS_ACTIVE_QTY > 0)
        ) AND W.CUS_CUST_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLAG_WINNER
    FROM `meli-bi-data.WHOWNER.DM_MP_INVESTMENTS_BY_PRODUCT` I
    LEFT JOIN winners_all W
        ON I.CUS_CUST_ID = W.CUS_CUST_ID
        AND I.SIT_SITE_ID = W.SIT_SITE_ID
        AND I.TIM_MONTH_ID = W.TIM_MONTH
    WHERE PRODUCT IN ('LCI_LCA', 'CDB', 'POTS', 'FUNDS', 'CRYPTO', 'ASSET')
        AND I.SIT_SITE_ID IN {sites}
        AND I.TIM_MONTH_ID >= {fecha_minima_month}
        AND I.TIM_MONTH_ID <= {fecha_maxima_month}
        -- Solo sellers que respondieron la encuesta NPS (reduce de ~260M a ~32K)
        AND I.CUS_CUST_ID IN (
            SELECT DISTINCT NPS_TX_CUS_CUST_ID
            FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL`
            WHERE NPS_TX_END_DATE >= '{fecha_minima}'
                AND NPS_TX_END_DATE < '{fecha_maxima}'
                AND NPS_TX_SIT_SITE_ID IN {sites}
                AND NPS_TX_NOTA_NPS IS NOT NULL
        )
    GROUP BY 1, 2, 3
)

SELECT
    CAST(TIM_MONTH AS STRING) AS TIM_MONTH,
    CUS_CUST_ID,
    SIT_SITE_ID,
    FLAG_INVERSIONES,
    FLAG_ASSET,
    FLAG_POTS AS FLAG_POTS_ACTIVO,
    FLAG_WINNER,
    CASE WHEN FLAG_INVERSIONES = 1 THEN 'Usa inversiones'
         ELSE 'No usa inversiones'
    END AS FLAG_USA_INVERSIONES
FROM flags_inversiones_por_mes
