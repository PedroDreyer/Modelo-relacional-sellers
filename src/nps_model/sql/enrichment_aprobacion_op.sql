-- ============================================
-- Enrichment: Tasa de Aprobación de Pagos para OP
-- ============================================
-- Join de sellers NPS con BT_SCO_ORIGIN_REPORT para calcular
-- tasa de aprobación (TPV aprobado / incoming total) por seller.
-- Solo aplica para OP (LINK + APICOW).
-- Parámetros: {sites}, {fecha_minima}, {fecha_maxima}

SELECT
    r.NPS_TX_CUS_CUST_ID AS CUS_CUST_ID,
    CAST(FORMAT_DATETIME('%Y%m', r.NPS_TX_END_DATE) AS INT64) AS TIM_MONTH,
    SUM(sor.pay_total_paid_dol_amt) AS incoming,
    SUM(CASE WHEN sor.pcc_status IN ('A','D') THEN sor.pay_total_paid_dol_amt ELSE 0 END) AS TPV,
    COUNT(*) AS qty,
    SAFE_DIVIDE(
        SUM(CASE WHEN sor.pcc_status IN ('A','D') THEN sor.pay_total_paid_dol_amt ELSE 0 END),
        SUM(sor.pay_total_paid_dol_amt)
    ) AS TASA_APROBACION
FROM `meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL` r
LEFT JOIN `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` sor
    ON sor.cus_cust_id_sel = r.NPS_TX_CUS_CUST_ID
WHERE 1=1
    AND r.NPS_TX_END_DATE BETWEEN '{fecha_minima}' AND '{fecha_maxima}'
    AND r.NPS_TX_SIT_SITE_ID IN {sites}
    AND (r.NPS_TX_E_CODE LIKE '%LINK%' OR r.NPS_TX_E_CODE LIKE '%API%')
    AND r.NPS IS NOT NULL
    AND sor.pay_created_dt >= DATE_SUB(DATE('{fecha_minima}'), INTERVAL 3 MONTH)
    AND sor.pay_created_dt < DATE('{fecha_maxima}')
    AND sor.pcc_status IN ('A','D','I','R','C')
    AND sor.sit_site_id IN {sites}
    AND sor.flow_type IN ('MI')
    AND sor.config_id IN ('OFF')
    AND sor.business_unit = 'ONLINE PAYMENTS'
    AND sor.pay_pm_type_id IN ('credit_card','debit_card','prepaid_card')
    AND sor.industry_id NOT IN ('test_low','product_low')
    AND (sor.ss_cust_segment_cross <> 'BIG SELLERS' OR sor.ss_cust_segment_cross IS NULL)
    AND sor.pay_try_last = 1
    AND (sor.product_type = 'LINK' OR sor.product_type = 'API' OR sor.product_type = 'COW')
GROUP BY 1, 2
