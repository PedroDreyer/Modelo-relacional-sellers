-- ============================================
-- Universo Total: Segmentación sobre base SEGMENTATION_SELLERS
-- ============================================
-- Base: SEGMENTATION_SELLERS (todos los sellers activos del producto)
-- Parámetros: {sites}, {fecha_minima_month}, {fecha_maxima_month}, {product_filter}

SELECT
    CAST(s.TIM_MONTH_TRANSACTION AS STRING) AS TIM_MONTH,
    CASE
        WHEN s.PRODUCTO = 'POINT' THEN 'Point'
        WHEN s.PRODUCTO = 'QR' THEN 'QR'
        WHEN s.PRODUCTO = 'TRANSFERENCIAS' THEN 'Transferencias'
        WHEN s.PRODUCTO IN ('LINK', 'APICOW') THEN 'OP'
        ELSE COALESCE(s.PRODUCTO, 'Sin dato')
    END AS PRODUCTO_PRINCIPAL,
    CASE
        WHEN s.NEW_MAS_FLAG = 1 THEN 'Newbie'
        WHEN s.NEW_MAS_FLAG = 0 THEN 'Legacy'
        ELSE 'Sin dato'
    END AS NEWBIE_LEGACY,
    CASE
        WHEN s.USO_PRODUCTOS = 'UNICO' AND s.TRANSF_FLAG = 1 THEN 'ONLY_TRANSFER'
        WHEN s.POINT_FLAG = 1 OR s.QR_FLAG = 1 OR s.LINK_FLAG = 1 OR s.API_FLAG = 1 THEN 'USA_SELLING_TOOL'
        ELSE 'Sin dato'
    END AS FLAG_ONLY_TRANSFER,
    COUNT(DISTINCT s.CUS_CUST_ID) AS total_sellers
FROM `meli-bi-data.SBOX_NPS_ANALYTICS.SEGMENTATION_SELLERS` s
WHERE s.SIT_SITE_ID IN {sites}
    AND s.TIM_MONTH_TRANSACTION >= {fecha_minima_month}
    AND s.TIM_MONTH_TRANSACTION <= {fecha_maxima_month}
    {product_filter}
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2
