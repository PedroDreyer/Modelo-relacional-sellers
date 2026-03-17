-- ============================================
-- Tabla: transacciones_sellers_segmentacion
-- Descripción: Transacciones de sellers con segmentación, productos utilizados y datos KYC (últimos 31 días)
-- ============================================

WITH STG_TRANSACCION AS (

WITH MASTER AS (
SELECT
    CUS_CUST_ID,                                              -- ID del cliente/seller
    CUST_SEGMENT_CROSS,                                       -- Segmento cruzado del cliente
    SIT_SITE_ID,                                              -- ID del sitio (país)
    cast(format_datetime("%Y%m", CURRENT_DATE) as INT64) AS TIM_MONTH,  -- Mes actual en formato YYYYMM
    CURRENT_DATE AS FECHA,                                    -- Fecha actual
    MAX(TIM_DAY) AS MAX_PAY_DATE,                             -- Fecha máxima de pago
    MIN(TIM_DAY) AS MIN_PAY_DATE,                             -- Fecha mínima de pago

    ROUND(SUM(TPN_POINT),0) AS TPN_POINT,                     -- Total transacciones Point
    ROUND(SUM(TPN_QR),0) AS TPN_QR,                           -- Total transacciones QR
    ROUND(SUM(TPN_TRS),0) AS TPN_TRS,                         -- Total transacciones Transferencias
    ROUND(SUM(TPN_OP_LINK),0) AS TPN_LINK,                    -- Total transacciones Link
    ROUND(SUM(TPN_OP_COW_API),0) AS TPN_API,                  -- Total transacciones API

    ROUND(SUM(TPV_POINT_DOL),0) AS TPV_POINT,                 -- TPV Point en dólares
    ROUND(SUM(TPV_QR_DOL),0) AS TPV_QR,                       -- TPV QR en dólares
    ROUND(SUM(TPV_POINT_DOL),0) AS TPV_POINT,                 -- TPV Point en dólares (duplicado)
    ROUND(SUM(TPV_OP_COW_API_DOL),0) AS TPV_API,              -- TPV API en dólares
    ROUND(SUM(TPV_OP_LINK_DOL),0) AS TPV_LINK,                -- TPV Link en dólares

FROM meli-bi-data.WHOWNER.LK_MP_MASTER_SELLERS
WHERE 1=1
    AND TIM_DAY BETWEEN CURRENT_DATE('-4')-31 AND CURRENT_DATE('-4')
GROUP BY 1,2,3,4
),

SELLERS_TRANSACTIONS AS (
SELECT
    DISTINCT
    t.CUS_CUST_ID,                                            -- ID del cliente/seller
    SIT_SITE_ID,                                              -- ID del sitio (país)
    cast(format_datetime("%Y%m", MAX_PAY_DATE) as INT64) AS TIM_MONTH,  -- Mes de transacción
    TPN_POINT,                                                -- Transacciones Point
    TPN_TRS,                                                  -- Transacciones Transferencias
    TPN_QR,                                                   -- Transacciones QR
    TPN_LINK,                                                 -- Transacciones Link
    TPN_API                                                   -- Transacciones API

FROM MASTER t
WHERE TPN_POINT > 0
    OR TPN_TRS > 0
    OR TPN_QR > 0
    OR TPN_LINK > 0
    OR TPN_API > 0
),

SEGMENTATION_SELLERS AS (
SELECT
    DISTINCT t.CUS_CUST_ID,                                   -- ID del cliente/seller
    t.SIT_SITE_ID,                                            -- ID del sitio (país)
    t.TIM_MONTH as TIM_MONTH_TRANSACTION,                     -- Mes de la transacción

    CASE WHEN TPN_POINT > 0 THEN 1 ELSE 0 END AS POINT_FLAG,  -- Flag uso de Point
    CASE WHEN TPN_TRS > 0 THEN 1 ELSE 0 END AS TRANSF_FLAG,   -- Flag uso de Transferencias
    CASE WHEN TPN_QR > 0 THEN 1 ELSE 0 END AS QR_FLAG,        -- Flag uso de QR
    CASE WHEN TPN_LINK > 0 THEN 1 ELSE 0 END AS LINK_FLAG,    -- Flag uso de Link
    CASE WHEN TPN_API > 0 THEN 1 ELSE 0 END AS API_FLAG,      -- Flag uso de API
    TPN_POINT,                                                -- Cantidad transacciones Point
    TPN_TRS,                                                  -- Cantidad transacciones Transferencias
    TPN_QR,                                                   -- Cantidad transacciones QR
    TPN_LINK,                                                 -- Cantidad transacciones Link
    TPN_API,                                                  -- Cantidad transacciones API

    s.CUST_SEGMENT_CROSS AS SEGMENTO,                         -- Segmento del seller
    s.CUST_SUB_SEGMENT_CROSS AS SUB_SEGMENTO,                 -- Sub-segmento del seller
    s.CUST_TYPE AS TIPO_SELLER,                               -- Tipo de seller
    s.NEW_MAS_FLAG,                                           -- Flag nuevo MAS
    s.TIM_MONTH AS TIM_MONTH_SEGMENTATION,                    -- Mes de segmentación
    CASE
        WHEN s.CUST_PRODUCT_DETAIL = 'OP_COW_API' THEN 'APICOW'
        WHEN s.CUST_PRODUCT_DETAIL = 'OP_LINK' THEN 'LINK'
        ELSE s.CUST_PRODUCT_DETAIL
    END AS PRODUCTO,                                          -- Producto principal normalizado

    CASE
        WHEN B.KYC_ENTITY_TYPE = 'person' THEN 'PF'
        WHEN B.KYC_ENTITY_TYPE = 'company' THEN 'PJ'
        ELSE B.KYC_ENTITY_TYPE
    END AS PERSON,                                            -- Tipo de persona (PF/PJ)

FROM SELLERS_TRANSACTIONS t
INNER JOIN meli-bi-data.WHOWNER.LK_MP_SEGMENTATION_SELLERS s
    ON t.CUS_CUST_ID = s.CUS_CUST_ID
    AND t.TIM_MONTH >= s.TIM_MONTH
    AND s.SELL_ACTIVE_MTD_FLAG = 1
LEFT JOIN meli-bi-data.WHOWNER.LK_KYC_VAULT_USER B
    ON B.CUS_CUST_ID = s.CUS_CUST_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY t.CUS_CUST_ID ORDER BY s.TIM_MONTH DESC) = 1
)

SELECT
    s.*,
    CASE WHEN POINT_FLAG + TRANSF_FLAG + QR_FLAG + LINK_FLAG + API_FLAG > 1
        THEN 'COMPARTIDO' ELSE 'UNICO'
    END AS USO_PRODUCTOS,                                     -- Uso de múltiples productos
    CASE
        WHEN PRODUCTO LIKE '%POINT%' AND POINT_FLAG = 1 THEN 'OK'
        WHEN PRODUCTO LIKE '%LINK%' AND LINK_FLAG = 1 THEN 'OK'
        WHEN PRODUCTO LIKE '%API%' AND API_FLAG = 1 THEN 'OK'
        WHEN PRODUCTO LIKE '%QR%' AND QR_FLAG = 1 THEN 'OK'
        WHEN PRODUCTO LIKE '%TRANSFERENCIA%' AND TRANSF_FLAG = 1 THEN 'OK'
        ELSE 'ERROR'
    END AS CHECK                                              -- Validación producto vs uso real

FROM SEGMENTATION_SELLERS s
WHERE
    PERSON IN ('PF', 'PJ') AND
    PRODUCTO IN ('POINT', 'QR', 'TRANSFERENCIAS', 'LINK', 'APICOW') AND
    SEGMENTO NOT IN ('BIG SELLERS')

)

-- Query principal: Segmentación final con flag de operaciones online
SELECT
    t.CUS_CUST_ID,                                            -- ID del cliente/seller
    CURRENT_DATE('-4') as FECHA,                              -- Fecha de ejecución
    t.SIT_SITE_ID,                                            -- ID del sitio (país)
    t.TIM_MONTH_TRANSACTION,                                  -- Mes de transacción
    t.POINT_FLAG,                                             -- Flag uso Point
    t.TRANSF_FLAG,                                            -- Flag uso Transferencias
    t.QR_FLAG,                                                -- Flag uso QR
    t.LINK_FLAG,                                              -- Flag uso Link
    t.API_FLAG,                                               -- Flag uso API
    t.TPN_POINT,                                              -- Cant. transacciones Point
    t.TPN_TRS,                                                -- Cant. transacciones Transferencias
    t.TPN_QR,                                                 -- Cant. transacciones QR
    t.TPN_LINK,                                               -- Cant. transacciones Link
    t.TPN_API,                                                -- Cant. transacciones API
    t.SEGMENTO,                                               -- Segmento del seller
    t.SUB_SEGMENTO,                                           -- Sub-segmento del seller
    t.TIPO_SELLER,                                            -- Tipo de seller
    t.NEW_MAS_FLAG,                                           -- Flag nuevo MAS
    t.TIM_MONTH_SEGMENTATION,                                 -- Mes de segmentación
    t.PRODUCTO,                                               -- Producto principal
    t.PERSON,                                                 -- Tipo persona (PF/PJ)
    t.USO_PRODUCTOS,                                          -- Uso único o compartido
    CASE WHEN LINK_FLAG > 1 OR API_FLAG > 1
        THEN 1 ELSE 0
    END AS OP_FLAG                                            -- Flag operaciones online (Link/API)
FROM STG_TRANSACCION t
WHERE CHECK = 'OK'
