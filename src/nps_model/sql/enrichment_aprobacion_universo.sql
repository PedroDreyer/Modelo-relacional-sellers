-- ============================================
-- Universo Total: Tasa de Aprobación sobre base SEGMENTATION_SELLERS
-- ============================================
-- Base: SEGMENTATION_SELLERS filtrado por OP (OP_FLAG=1)
-- Join: BT_SCO_ORIGIN_REPORT para calcular tasa de aprobación
-- Parámetros: {sites}, {fecha_minima}, {fecha_maxima}, {fecha_minima_month}, {fecha_maxima_month}

WITH seller_aprobacion AS (
    SELECT
        sor.cus_cust_id_sel AS CUS_CUST_ID,
        CAST(FORMAT_DATE('%Y%m', DATE_TRUNC(sor.pay_created_dt, MONTH)) AS INT64) AS TIM_MONTH,
        SAFE_DIVIDE(
            SUM(CASE WHEN sor.pcc_status IN ('A','D') THEN sor.pay_total_paid_dol_amt ELSE 0 END),
            SUM(sor.pay_total_paid_dol_amt)
        ) AS TASA_APROBACION
    FROM `meli-bi-data.WHOWNER.BT_SCO_ORIGIN_REPORT` sor
    WHERE sor.pay_created_dt >= DATE('{fecha_minima}')
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
)

SELECT
    CAST(s.TIM_MONTH_TRANSACTION AS STRING) AS TIM_MONTH,
    CASE
        WHEN a.TASA_APROBACION IS NULL THEN 'Sin datos'
        WHEN a.TASA_APROBACION >= 0.95 THEN 'Alta (≥95%)'
        WHEN a.TASA_APROBACION >= 0.85 THEN 'Media (85-95%)'
        ELSE 'Baja (<85%)'
    END AS RANGO_APROBACION,
    COUNT(DISTINCT s.CUS_CUST_ID) AS total_sellers
FROM `meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` s
LEFT JOIN seller_aprobacion a
    ON s.CUS_CUST_ID = a.CUS_CUST_ID
    AND s.TIM_MONTH_TRANSACTION = a.TIM_MONTH
WHERE s.SIT_SITE_ID IN {sites}
    AND s.TIM_MONTH_TRANSACTION >= {fecha_minima_month}
    AND s.TIM_MONTH_TRANSACTION <= {fecha_maxima_month}
    AND s.OP_FLAG = 1
GROUP BY 1, 2
ORDER BY 1, 2
