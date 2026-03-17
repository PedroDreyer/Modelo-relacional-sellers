-- ============================================
-- Tabla: Credits
-- Descripción: Tabla con detalle por CUS_CUST_ID de Oferta de Créditos, flags de offers y uso de productos financieros
-- ============================================

    SELECT
        f.CUS_CUST_ID,                             -- ID del cliente (Customer)
        f.TIM_MONTH,                                  -- Mes en formato YYYYMM    
        f.CREDIT_GROUP as CREDIT_GROUP,            -- Grupo de crédito
        f.FLAG_OFFER_CREDITS,                      -- Flag oferta de créditos
        f.FLAG_ROW_PERSONAL_LOAN,                  -- Flag fila préstamo personal
        f.FLAG_OFFER_MCREDITS,                     -- Flag oferta MCredits
        f.FLAG_TC_OFFER,                           -- Flag oferta tarjeta de crédito
        f.PRODUCT_USED.MERCHANT_CREDIT AS USO_MC,  -- Uso de Merchant Credit
        f.PRODUCT_USED.SECURED_LOAN AS USO_SL,     -- Uso de Secured Loan
        f.PRODUCT_USED.CONSUMER_CREDIT AS USO_CC,  -- Uso de Consumer Credit
        f.PRODUCT_USED.PERSONAL_LOAN AS USO_PL,    -- Uso de Personal Loan
        f.PRODUCT_USED.CREDIT_CARD AS USO_TC       -- Uso de Tarjeta de Crédito
    FROM `meli-bi-data.WHOWNER.LK_MP_MAUS_CREDIT_PROFILE` f
