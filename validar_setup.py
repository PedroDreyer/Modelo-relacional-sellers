"""
Script de Validación Completa del Setup
========================================

Ejecuta este script ANTES de ejecutar el modelo por primera vez.
Valida todos los requisitos y configuraciones necesarias.

Uso:
    python validar_setup.py
"""

import sys
import subprocess
from pathlib import Path
import importlib.util

# Colores para terminal
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_header(text):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}\n")

def print_step(number, text):
    print(f"\n{Colors.BOLD}📋 Paso {number}: {text}{Colors.END}")

def print_success(text):
    print(f"{Colors.GREEN}✅ {text}{Colors.END}")

def print_error(text):
    print(f"{Colors.RED}❌ {text}{Colors.END}")

def print_warning(text):
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")

def print_info(text):
    print(f"   {text}")

# Variables globales para tracking
errores = []
advertencias = []

def validar_python():
    """Valida versión de Python"""
    print_step(1, "Validando versión de Python")
    
    version = sys.version_info
    version_str = f"{version.major}.{version.minor}.{version.micro}"
    print_info(f"Versión detectada: Python {version_str}")
    
    if version.major >= 3 and version.minor >= 10:
        print_success("Versión de Python válida (>= 3.10)")
        return True
    else:
        print_error(f"Se requiere Python 3.10 o superior (tienes {version_str})")
        errores.append("Python version insuficiente")
        return False

def validar_pip():
    """Valida que pip esté instalado"""
    print_step(2, "Validando pip (gestor de paquetes)")
    
    try:
        result = subprocess.run(['pip', '--version'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print_info(result.stdout.strip())
            print_success("pip está instalado")
            return True
        else:
            print_error("pip no está instalado correctamente")
            errores.append("pip no disponible")
            return False
    except Exception as e:
        print_error(f"No se pudo ejecutar pip: {e}")
        errores.append("pip no disponible")
        return False

def validar_dependencias():
    """Valida que las dependencias críticas estén instaladas"""
    print_step(3, "Validando dependencias críticas")
    
    dependencias_criticas = {
        'pandas': 'pandas',
        'google.cloud.bigquery': 'google-cloud-bigquery',
        'google.cloud.bigquery_storage': 'google-cloud-bigquery-storage',
        'pyarrow': 'pyarrow',
        'yaml': 'pyyaml',
        'matplotlib': 'matplotlib',
        'numpy': 'numpy'
    }
    
    instaladas = []
    faltantes = []
    
    for modulo, nombre_paquete in dependencias_criticas.items():
        if importlib.util.find_spec(modulo.split('.')[0]) is not None:
            instaladas.append(nombre_paquete)
            print_success(f"{nombre_paquete} instalado")
        else:
            faltantes.append(nombre_paquete)
            print_error(f"{nombre_paquete} NO instalado")
    
    if faltantes:
        print_warning(f"\n⚠️  Faltan {len(faltantes)} dependencias")
        print_info("Para instalar todas las dependencias, ejecuta:")
        print_info(f"   {Colors.BOLD}pip install -e .{Colors.END}")
        errores.append(f"Dependencias faltantes: {', '.join(faltantes)}")
        return False
    else:
        print_success(f"Todas las dependencias críticas instaladas ({len(instaladas)}/7)")
        return True

def validar_estructura_proyecto():
    """Valida que existan las carpetas y archivos necesarios"""
    print_step(4, "Validando estructura del proyecto")
    
    project_root = Path(__file__).parent
    
    rutas_requeridas = {
        'config/config.yaml': 'Archivo de configuración',
        'src/nps_model/': 'Código fuente del modelo',
        'scripts/': 'Scripts de checkpoints',
        'outputs/': 'Carpeta de salida (se crea si no existe)',
        'ejecutar_modelo_completo.py': 'Script principal de ejecución'
    }
    
    todo_ok = True
    
    for ruta, descripcion in rutas_requeridas.items():
        ruta_completa = project_root / ruta
        
        # Crear carpeta outputs si no existe
        if ruta == 'outputs/' and not ruta_completa.exists():
            ruta_completa.mkdir(parents=True, exist_ok=True)
            print_warning(f"Carpeta 'outputs/' creada (no existía)")
            continue
        
        if ruta_completa.exists():
            print_success(f"{ruta} - {descripcion}")
        else:
            print_error(f"{ruta} - NO ENCONTRADO")
            errores.append(f"Falta: {ruta}")
            todo_ok = False
    
    if todo_ok:
        print_success("Estructura del proyecto válida")
    
    return todo_ok

def validar_config_yaml():
    """Valida que config.yaml tenga los campos requeridos"""
    print_step(5, "Validando archivo de configuración")
    
    project_root = Path(__file__).parent
    config_path = project_root / 'config' / 'config.yaml'
    
    if not config_path.exists():
        print_error("No se encontró config/config.yaml")
        errores.append("config.yaml no existe")
        return False
    
    try:
        import yaml
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Validar campos requeridos
        campos_requeridos = ['sites', 'quarter_actual', 'quarter_anterior']
        campos_faltantes = []
        
        for campo in campos_requeridos:
            if campo not in config:
                campos_faltantes.append(campo)
        
        if campos_faltantes:
            print_error(f"Faltan campos en config.yaml: {', '.join(campos_faltantes)}")
            errores.append("config.yaml incompleto")
            return False
        
        # Mostrar configuración actual
        print_info(f"Site configurado: {config['sites']}")
        print_info(f"Quarters: {config['quarter_anterior']} vs {config['quarter_actual']}")
        print_success("config.yaml válido")
        return True
        
    except Exception as e:
        print_error(f"Error al leer config.yaml: {e}")
        errores.append("config.yaml con errores")
        return False

def validar_credenciales_gcloud():
    """Valida credenciales de Google Cloud"""
    print_step(6, "Validando credenciales de Google Cloud")
    
    try:
        result = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'],
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 and result.stdout.strip():
            print_success("Credenciales de Google Cloud configuradas")
            print_info("Token de acceso obtenido correctamente")
            return True
        else:
            print_error("No se pudieron obtener credenciales")
            print_warning("Para configurar credenciales, ejecuta:")
            print_info(f"   {Colors.BOLD}gcloud auth application-default login{Colors.END}")
            errores.append("Credenciales no configuradas")
            return False
            
    except FileNotFoundError:
        print_error("gcloud CLI no está instalado")
        print_warning("Descarga e instala Google Cloud SDK desde:")
        print_info("   https://cloud.google.com/sdk/docs/install")
        errores.append("gcloud CLI no instalado")
        return False
    except Exception as e:
        print_error(f"Error al verificar credenciales: {e}")
        advertencias.append("No se pudo verificar credenciales")
        return False

def validar_conexion_bigquery():
    """Valida conexión a BigQuery"""
    print_step(7, "Validando conexión a BigQuery")
    
    try:
        from google.cloud import bigquery
        
        # Intentar crear cliente
        client = bigquery.Client(project='meli-bi-data')
        print_success("Cliente de BigQuery creado exitosamente")
        
        # Intentar una query simple
        query = "SELECT 1 as test LIMIT 1"
        result = client.query(query).result()
        
        for row in result:
            if row.test == 1:
                print_success("Conexión a BigQuery verificada (query de prueba exitosa)")
                return True
        
        print_error("Query de prueba falló")
        errores.append("BigQuery no responde correctamente")
        return False
        
    except Exception as e:
        print_error(f"No se pudo conectar a BigQuery: {e}")
        print_warning("Verifica:")
        print_info("   1. Que tengas credenciales configuradas")
        print_info("   2. Que tengas permisos en el proyecto 'meli-bi-data'")
        print_info("   3. Que estés conectado a internet")
        errores.append(f"Error de conexión BigQuery: {str(e)[:100]}")
        return False

def validar_acceso_tablas():
    """Valida acceso a tablas críticas de BigQuery"""
    print_step(8, "Validando acceso a tablas críticas")
    
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project='meli-bi-data')
        
        # Tabla principal de NPS Relacional Sellers
        tabla_nps = "meli-bi-data.SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL"
        
        print_info(f"Verificando acceso a: {tabla_nps}")
        
        # Query simple para verificar acceso
        query = f"""
        SELECT COUNT(*) as total
        FROM `{tabla_nps}`
        LIMIT 1
        """
        
        result = client.query(query).result()
        
        for row in result:
            count = row.total
            print_success(f"Acceso verificado ({count:,} registros)")
            return True
        
        print_error("No se pudo verificar el acceso a la tabla")
        errores.append("Tabla principal no accesible")
        return False
        
    except Exception as e:
        print_error(f"Error al verificar acceso a tablas: {e}")
        print_warning("Verifica que tengas permisos de lectura en:")
        print_info(f"   - Proyecto: meli-bi-data")
        print_info(f"   - Tabla: SBOX_CX_BI_ADS_CORE.BT_NPS_TX_SELLERS_MP_DETAIL")
        errores.append(f"Error de acceso a tablas: {str(e)[:100]}")
        return False

def generar_resumen():
    """Genera resumen final de la validación"""
    print_header("📊 RESUMEN DE VALIDACIÓN")
    
    total_pasos = 8
    pasos_exitosos = total_pasos - len(errores)
    
    print(f"\n{Colors.BOLD}Resultado:{Colors.END}")
    print(f"   ✅ Pasos exitosos: {pasos_exitosos}/{total_pasos}")
    
    if errores:
        print(f"\n{Colors.RED}{Colors.BOLD}❌ ERRORES CRÍTICOS ({len(errores)}):{Colors.END}")
        for i, error in enumerate(errores, 1):
            print(f"   {i}. {error}")
    
    if advertencias:
        print(f"\n{Colors.YELLOW}{Colors.BOLD}⚠️  ADVERTENCIAS ({len(advertencias)}):{Colors.END}")
        for i, warning in enumerate(advertencias, 1):
            print(f"   {i}. {warning}")
    
    print("\n" + "="*80)
    
    if not errores:
        print(f"\n{Colors.GREEN}{Colors.BOLD}🎉 ¡TODO LISTO! Puedes ejecutar el modelo{Colors.END}\n")
        print(f"{Colors.BOLD}Siguiente paso:{Colors.END}")
        print(f"   python ejecutar_modelo_completo.py\n")
        return True
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}⚠️  CORRIGE LOS ERRORES ANTES DE CONTINUAR{Colors.END}\n")
        print(f"{Colors.BOLD}Pasos sugeridos:{Colors.END}")
        
        if "pip no disponible" in str(errores):
            print("   1. Instala pip")
        
        if "Dependencias faltantes" in str(errores):
            print("   2. Instala dependencias: pip install -e .")
        
        if "Credenciales no configuradas" in str(errores):
            print("   3. Configura credenciales: gcloud auth application-default login")
        
        if "BigQuery" in str(errores):
            print("   4. Verifica permisos en BigQuery (proyecto: meli-bi-data)")
        
        print(f"\n   5. Vuelve a ejecutar: python validar_setup.py\n")
        return False

def main():
    """Función principal"""
    print_header("🔍 VALIDACIÓN COMPLETA DEL SETUP - Modelo NPS Relacional Sellers")
    
    print(f"{Colors.BOLD}Este script validará:{Colors.END}")
    print("   • Versión de Python y dependencias")
    print("   • Estructura del proyecto")
    print("   • Configuración (config.yaml)")
    print("   • Credenciales de Google Cloud")
    print("   • Conexión a BigQuery")
    print("\n" + "="*80)
    
    # Ejecutar validaciones
    validar_python()
    validar_pip()
    validar_dependencias()
    validar_estructura_proyecto()
    validar_config_yaml()
    validar_credenciales_gcloud()
    validar_conexion_bigquery()
    validar_acceso_tablas()
    
    # Generar resumen
    resultado = generar_resumen()
    
    # Exit code
    sys.exit(0 if resultado else 1)

if __name__ == "__main__":
    main()
