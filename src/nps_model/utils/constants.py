"""
Constantes del modelo NPS Relacional Sellers
"""

# ==========================================
# CONFIGURACIÓN DE NPS
# ==========================================
# Valores de NPS
NPS_DETRACTOR = -1
NPS_NEUTRO = 0
NPS_PROMOTOR = 1

# ==========================================
# CONFIGURACIÓN DE ANÁLISIS
# ==========================================
# Número de meses históricos para gráficos de evolución
MESES_EVOLUCION_NPS = 13  # 12 meses atrás + mes actual

# Número de meses para gráficos de quejas/drivers
MESES_ANALISIS_DRIVERS = 6

# Número máximo de comentarios por motivo para análisis cualitativo
MAX_COMENTARIOS_ANALISIS = 200

# Top N motivos para análisis detallado
TOP_N_MOTIVOS = 3

# ==========================================
# UMBRALES (CONFIGURABLES CON DEFAULTS)
# ==========================================
# Umbral para variación significativa en tendencias (en puntos porcentuales)
UMBRAL_VARIACION_TENDENCIA = 0.5

# Umbral para mostrar etiquetas en gráficos (%)
UMBRAL_ETIQUETA_GRAFICO = 2.0

# Umbral para considerar variación "significativa" en resumen (pp)
UMBRAL_VARIACION_SIGNIFICATIVA = 0.5

# ==========================================
# DIMENSIONES DE SEGMENTACIÓN DE SELLERS
# ==========================================

# Base dimensions (from NPS survey table)
DIMENSIONES_BASE = [
    "SEGMENTO_TAMANO_SELLER",
    "SEGMENTO_CROSSMP",
    "POINT_DEVICE_TYPE",
    "E_CODE",
    "PF_PJ",
]

# Enrichment: segmentation dimensions
DIMENSIONES_SEGMENTACION = [
    "PRODUCTO_PRINCIPAL",    # POINT, QR, OP, TRANSFERENCIAS
    "NEWBIE_LEGACY",         # Newbie vs Legacy (from NEW_MAS_FLAG)
    "REGION",                # Region (if available)
    "FLAG_ONLY_TRANSFER",    # ONLY_TR vs USA_SELLING_TOOL
]

# Enrichment: transaction dimensions
DIMENSIONES_TRANSACCIONES = [
    "RANGO_TPV",             # TPV bucket
    "RANGO_TPN",             # TPN bucket
]

# Enrichment: credits dimensions
DIMENSIONES_CREDITS = [
    "FLAG_USA_CREDITO",
    "FLAG_TARJETA_CREDITO",
    "ESTADO_OFERTA_CREDITO",
    "CREDIT_GROUP",
]

# Enrichment: investment dimensions
DIMENSIONES_INVERSIONES = [
    "FLAG_USA_INVERSIONES",
    "FLAG_POTS_ACTIVO",
    "FLAG_INVERSIONES",
    "FLAG_ASSET",
    "FLAG_WINNER",
]

# Enrichment: Top Off / atención al cliente
DIMENSIONES_TOPOFF = [
    "FLAG_TOPOFF",
]

# Enrichment: aprobación de pagos (OP only)
DIMENSIONES_APROBACION = [
    "RANGO_APROBACION",
]

# Device / problemas de funcionamiento (from NPS survey table)
DIMENSIONES_DEVICE = [
    "MODELO_DEVICE",
    "PROBLEMA_FUNCIONAMIENTO",
    "TIPO_PROBLEMA",
]

# Valoraciones de device Point (escala numérica)
VALORACIONES_DEVICE = [
    "VALORAC_BLUETOOTH",
    "VALORAC_CHIP",
    "VALORAC_WIFI",
    "VALORAC_MEDIO_PAGO",
    "VALORAC_LECTURA",
    "VALORAC_PROCESAMIENTO",
    "VALORAC_SEGURIDAD",
    "VALORAC_BATERIA",
    "VALORAC_COMPROBANTES",
    "VALORAC_CONECTIVIDAD",
]

# All dimensions (union)
DIMENSIONES_SELLERS = (
    DIMENSIONES_BASE
    + DIMENSIONES_SEGMENTACION
    + DIMENSIONES_TRANSACCIONES
    + DIMENSIONES_CREDITS
    + DIMENSIONES_INVERSIONES
    + DIMENSIONES_TOPOFF
    + DIMENSIONES_APROBACION
    + DIMENSIONES_DEVICE
)

# Mapping: config flag name -> column name
DIMENSION_CONFIG_MAP = {
    "analizar_segmento_tamano": "SEGMENTO_TAMANO_SELLER",
    "analizar_segmento_crossmp": "SEGMENTO_CROSSMP",
    "analizar_point_device_type": "POINT_DEVICE_TYPE",
    "analizar_e_code": "E_CODE",
    "analizar_pf_pj": "PF_PJ",
    "analizar_producto_principal": "PRODUCTO_PRINCIPAL",
    "analizar_newbie_legacy": "NEWBIE_LEGACY",
    "analizar_region": "REGION",
    "analizar_rango_tpv": "RANGO_TPV",
    "analizar_rango_tpn": "RANGO_TPN",
    "analizar_uso_credito": "FLAG_USA_CREDITO",
    "analizar_tarjeta_credito": "FLAG_TARJETA_CREDITO",
    "analizar_estado_oferta_credito": "ESTADO_OFERTA_CREDITO",
    "analizar_credit_group": "CREDIT_GROUP",
    "analizar_uso_inversiones": "FLAG_USA_INVERSIONES",
    "analizar_pots_activo": "FLAG_POTS_ACTIVO",
    "analizar_inversiones_flag": "FLAG_INVERSIONES",
    "analizar_asset_flag": "FLAG_ASSET",
    "analizar_winner_flag": "FLAG_WINNER",
    "analizar_topoff": "FLAG_TOPOFF",
    "analizar_tasa_aprobacion": "RANGO_APROBACION",
    "analizar_only_transfer": "FLAG_ONLY_TRANSFER",
    "analizar_modelo_device": "MODELO_DEVICE",
    "analizar_problema_funcionamiento": "PROBLEMA_FUNCIONAMIENTO",
    "analizar_tipo_problema": "TIPO_PROBLEMA",
}

# Agrupar motivos de seguridad en uno solo (Seguridad)
# Incluye "Seguridad de la cuenta", "Falta de seguridad en/de la cuenta", etc.
MOTIVO_NORMALIZACION_SEGURIDAD = {
    "Falta de seguridad de la cuenta": "Seguridad",
    "Falta de seguridad en la cuenta": "Seguridad",
    "Falta de segurança da conta": "Seguridad",
    "Falta de segurança da conta.": "Seguridad",
    "Seguridad": "Seguridad",
    "Seguridad de la cuenta": "Seguridad",
    "Segurança da conta": "Seguridad",
}

# ==========================================
# COLORES PARA VISUALIZACIONES
# ==========================================
COLORES_SITES = {
    "MLB": "#00a650",  # Brasil - Verde
    "MLM": "#dc3545",  # México - Rojo
    "MLA": "#00bfff",  # Argentina - Celeste
    "MLC": "#dc3545",  # Chile - Rojo
    "MPE": "#dc3545",  # Perú - Rojo
    "MLU": "#3483fa",  # Uruguay - Azul
    "MCO": "#ffe600",  # Colombia - Amarillo
}

# ==========================================
# CONFIGURACIÓN DE BIGQUERY
# ==========================================
# Tabla de NPS Relacional Sellers
NPS_SELLERS_TABLE = "meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL"

# Fecha mínima de datos (por defecto)
FECHA_MINIMA_DEFAULT = "2024-11-01"
