-- ============================================
-- Enriquecimiento: Restricciones de Sellers (para OP)
-- ============================================
-- Calcula CONSIDERACION_AJUSTADA por response_id.
-- Sellers con tarjeta roja/amarilla (COLOR 2,3) y sentencia activa → excluir (flag=0)
-- Solo se aplica para updates LINK/APICOW.
-- Join key: SURVEY_ID (NPS_TX_QUALTRICS_RESPONSE_ID)

SELECT
    CAST(r.NPS_TX_QUALTRICS_RESPONSE_ID AS STRING) AS SURVEY_ID,
    MAX(RR.COLOR_DE_TARJETA) OVER (PARTITION BY r.NPS_TX_QUALTRICS_RESPONSE_ID) AS COLOR_DE_TARJETA,
    MAX(
        CASE WHEN RR.SENTENCE_ID IS NULL THEN 0 ELSE 1 END
    ) OVER (PARTITION BY r.NPS_TX_QUALTRICS_RESPONSE_ID) AS USER_RESTRICCION,
    MIN(
        CASE
            WHEN RR.COLOR_DE_TARJETA IN ('2', '3')
                AND (
                    RR.SENTENCE_REHABILITATION_DATE IS NULL
                    OR RR.SENTENCE_REHABILITATION_DATE >= r.NPS_TX_END_DATE
                )
            THEN 0
            ELSE 1
        END
    ) OVER (PARTITION BY r.NPS_TX_QUALTRICS_RESPONSE_ID) AS CONSIDERACION_AJUSTADA

FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL` r

LEFT JOIN `meli-bi-data.WHOWNER.BT_RES_RESTRICTIONS_SENTENCES` RR
    ON RR.user_id = r.NPS_TX_CUS_CUST_ID
    AND r.NPS_TX_END_DATE >= RR.SENTENCE_DATE
    AND RR.SENTENCE_DATE >= DATE_SUB(r.NPS_TX_END_DATE, INTERVAL 6 MONTH)

WHERE 1=1
    AND r.NPS_TX_END_DATE >= '{fecha_minima}'
    AND r.NPS_TX_END_DATE < '{fecha_maxima}'
    AND r.NPS_TX_SIT_SITE_ID IN {sites}
    AND r.NPS_TX_NOTA_NPS IS NOT NULL
    {e_code_filter}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.NPS_TX_QUALTRICS_RESPONSE_ID
    ORDER BY r.NPS_TX_END_DATE DESC, RR.SENTENCE_DATE DESC
) = 1
