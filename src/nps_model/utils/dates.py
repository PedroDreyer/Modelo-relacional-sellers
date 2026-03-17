"""
Utilidades para manejo de fechas en formato YYYYMM
"""

from datetime import datetime
from typing import List


def calcular_mes_anterior(mes_str: str) -> str:
    """
    Calcula el mes anterior a partir de formato YYYYMM.
    
    Args:
        mes_str: Mes en formato YYYYMM (ej: "202512")
    
    Returns:
        Mes anterior en formato YYYYMM (ej: "202511")
    
    Examples:
        >>> calcular_mes_anterior("202512")
        '202511'
        >>> calcular_mes_anterior("202501")
        '202412'
    """
    year = int(mes_str[:4])
    month = int(mes_str[4:])

    month -= 1
    if month == 0:
        month = 12
        year -= 1

    return f"{year}{month:02d}"


def calcular_mes_año_anterior(mes_str: str) -> str:
    """
    Calcula el mismo mes del año anterior (YoY).
    
    Args:
        mes_str: Mes en formato YYYYMM (ej: "202512")
    
    Returns:
        Mes año anterior en formato YYYYMM (ej: "202412")
    
    Examples:
        >>> calcular_mes_año_anterior("202512")
        '202412'
    """
    year = int(mes_str[:4])
    month = int(mes_str[4:])

    year -= 1

    return f"{year}{month:02d}"


def calcular_meses_atras(mes_str: str, n_meses: int) -> str:
    """
    Calcula N meses atrás desde un mes dado.
    
    Args:
        mes_str: Mes en formato YYYYMM (ej: "202512")
        n_meses: Número de meses a retroceder
    
    Returns:
        Mes resultado en formato YYYYMM
    
    Examples:
        >>> calcular_meses_atras("202512", 12)
        '202412'
        >>> calcular_meses_atras("202503", 5)
        '202410'
    """
    year = int(mes_str[:4])
    month = int(mes_str[4:])

    month -= n_meses
    while month <= 0:
        month += 12
        year -= 1

    return f"{year}{month:02d}"


def generar_rango_meses(start_month_str: str, end_month_str: str) -> List[str]:
    """
    Genera lista de meses entre start y end (inclusive).
    
    Args:
        start_month_str: Mes inicio en formato YYYYMM
        end_month_str: Mes fin en formato YYYYMM
    
    Returns:
        Lista de meses en formato YYYYMM
    
    Examples:
        >>> generar_rango_meses("202501", "202503")
        ['202501', '202502', '202503']
    """
    meses = []
    start_year = int(start_month_str[:4])
    start_month = int(start_month_str[4:])
    end_year = int(end_month_str[:4])
    end_month = int(end_month_str[4:])

    current_year = start_year
    current_month = start_month

    while current_year < end_year or (current_year == end_year and current_month <= end_month):
        meses.append(f"{current_year}{current_month:02d}")
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    return meses


def convertir_mes_a_texto(mes_str: str, formato: str = "corto") -> str:
    """
    Convierte mes formato YYYYMM a texto legible.
    
    Args:
        mes_str: Mes en formato YYYYMM (ej: "202512")
        formato: "corto" (Dic 2025) o "largo" (Diciembre 2025)
    
    Returns:
        Mes en formato texto
    
    Examples:
        >>> convertir_mes_a_texto("202512", "corto")
        'Dic 2025'
        >>> convertir_mes_a_texto("202512", "largo")
        'Diciembre 2025'
    """
    try:
        year = mes_str[:4]
        month_num = int(mes_str[4:])

        meses_cortos = {
            1: "Ene",
            2: "Feb",
            3: "Mar",
            4: "Abr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Ago",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dic",
        }

        meses_largos = {
            1: "Enero",
            2: "Febrero",
            3: "Marzo",
            4: "Abril",
            5: "Mayo",
            6: "Junio",
            7: "Julio",
            8: "Agosto",
            9: "Septiembre",
            10: "Octubre",
            11: "Noviembre",
            12: "Diciembre",
        }

        if formato == "largo":
            return f"{meses_largos[month_num]} {year}"
        else:
            return f"{meses_cortos[month_num]} {year}"

    except Exception:
        return mes_str


def meses_del_trimestre(mes_str: str) -> List[str]:
    """
    Devuelve los 3 meses (YYYYMM) del trimestre al que pertenece mes_str.
    Ej: "202602" -> ["202601", "202602", "202603"]
    """
    year = int(mes_str[:4])
    month = int(mes_str[4:])
    # Mes de inicio del trimestre (1, 4, 7, 10)
    trimestre_inicio = ((month - 1) // 3) * 3 + 1
    return [
        f"{year}{trimestre_inicio:02d}",
        f"{year}{trimestre_inicio + 1:02d}",
        f"{year}{trimestre_inicio + 2:02d}",
    ]


def meses_trimestre_anterior(mes_str: str) -> List[str]:
    """
    Devuelve los 3 meses del trimestre anterior.
    Ej: "202602" -> ["202510", "202511", "202512"]
    """
    year = int(mes_str[:4])
    month = int(mes_str[4:])
    trimestre_inicio = ((month - 1) // 3) * 3 + 1
    if trimestre_inicio == 1:
        return [f"{year - 1}10", f"{year - 1}11", f"{year - 1}12"]
    return [
        f"{year}{trimestre_inicio - 3:02d}",
        f"{year}{trimestre_inicio - 2:02d}",
        f"{year}{trimestre_inicio - 1:02d}",
    ]


def parse_quarter(q_str: str) -> tuple[int, int]:
    """
    Parsea formato quarter "YYQ[1-4]" a (año_completo, num_quarter).

    Examples:
        >>> parse_quarter("26Q1")
        (2026, 1)
        >>> parse_quarter("25Q4")
        (2025, 4)
    """
    q_str = q_str.strip().upper()
    if len(q_str) != 4 or q_str[2] != 'Q':
        raise ValueError(f"Formato quarter inválido: '{q_str}'. Usar YYQ[1-4], ej: 26Q1")
    year_short = int(q_str[:2])
    q_num = int(q_str[3])
    if q_num < 1 or q_num > 4:
        raise ValueError(f"Quarter debe ser 1-4, recibido: {q_num}")
    year = 2000 + year_short
    return year, q_num


def quarter_to_months(q_str: str) -> List[str]:
    """
    Convierte quarter a lista de 3 meses YYYYMM.

    Examples:
        >>> quarter_to_months("26Q1")
        ['202601', '202602', '202603']
        >>> quarter_to_months("25Q4")
        ['202510', '202511', '202512']
    """
    year, q_num = parse_quarter(q_str)
    start_month = (q_num - 1) * 3 + 1
    return [f"{year}{start_month + i:02d}" for i in range(3)]


def quarter_fecha_final(q_str: str) -> str:
    """
    Devuelve el último mes del quarter en formato YYYYMM.

    Examples:
        >>> quarter_fecha_final("26Q1")
        '202603'
        >>> quarter_fecha_final("25Q4")
        '202512'
    """
    return quarter_to_months(q_str)[-1]


def quarters_to_month_range(q_anterior: str, q_actual: str) -> tuple[int, int]:
    """
    Devuelve (mes_min, mes_max) como int YYYYMM cubriendo ambos quarters.

    Examples:
        >>> quarters_to_month_range("25Q4", "26Q1")
        (202510, 202603)
    """
    meses_ant = quarter_to_months(q_anterior)
    meses_act = quarter_to_months(q_actual)
    return int(meses_ant[0]), int(meses_act[-1])


def quarters_to_date_range(q_anterior: str, q_actual: str) -> tuple[str, str]:
    """
    Devuelve (fecha_min, fecha_max) en formato YYYY-MM-DD cubriendo ambos quarters.
    fecha_max es el primer día del mes siguiente al último mes (exclusive end).

    Examples:
        >>> quarters_to_date_range("25Q4", "26Q1")
        ('2025-10-01', '2026-04-01')
    """
    mes_min, mes_max = quarters_to_month_range(q_anterior, q_actual)
    min_str = str(mes_min)
    max_str = str(mes_max)
    fecha_min = f"{min_str[:4]}-{min_str[4:]}-01"
    year_max = int(max_str[:4])
    month_max = int(max_str[4:])
    if month_max == 12:
        fecha_max = f"{year_max + 1}-01-01"
    else:
        fecha_max = f"{year_max}-{month_max + 1:02d}-01"
    return fecha_min, fecha_max


def quarter_label(q_str: str) -> str:
    """
    Formato legible de un quarter.

    Examples:
        >>> quarter_label("26Q1")
        'Q1 2026'
        >>> quarter_label("25Q4")
        'Q4 2025'
    """
    year, q_num = parse_quarter(q_str)
    return f"Q{q_num} {year}"


def validar_formato_quarter(q_str: str) -> bool:
    """Valida si un string tiene formato YYQ[1-4] válido."""
    try:
        parse_quarter(q_str)
        return True
    except (ValueError, TypeError):
        return False


def validar_formato_mes(mes_str: str) -> bool:
    """
    Valida si un string tiene formato YYYYMM válido.
    
    Args:
        mes_str: String a validar
    
    Returns:
        True si es formato válido, False en caso contrario
    
    Examples:
        >>> validar_formato_mes("202512")
        True
        >>> validar_formato_mes("202513")
        False
        >>> validar_formato_mes("20251")
        False
    """
    try:
        if len(mes_str) != 6:
            return False

        year = int(mes_str[:4])
        month = int(mes_str[4:])

        if year < 2000 or year > 2100:
            return False

        if month < 1 or month > 12:
            return False

        return True

    except (ValueError, TypeError):
        return False
