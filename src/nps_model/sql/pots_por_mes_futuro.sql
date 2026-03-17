-- ============================================
-- Tabla: pots_por_mes
-- Descripción: CTE que identifica sellers con POTS activo por mes
-- ============================================

Pots_Per_Month AS (
    SELECT DISTINCT
        CAST(FORMAT_DATE('%Y%m', A.TIM_DAY) AS INT64) AS TIM_MONTH,  -- Mes en formato YYYYMM
        A.CUS_CUST_ID,   -- ID único del seller
        A.SIT_SITE_ID    -- ID del sitio
    FROM `meli-bi-data.WHOWNER.DM_MP_INVESTMENTS` A
    WHERE A.POTS_ACTIVE_QTY > 0              -- Solo sellers con POTS activo
        AND A.SIT_SITE_ID = site_code        -- Filtro por sitio (parámetro)
        AND A.TIM_DAY BETWEEN DATE '2024-01-01' AND CURRENT_DATE()  -- Desde 2024
)
