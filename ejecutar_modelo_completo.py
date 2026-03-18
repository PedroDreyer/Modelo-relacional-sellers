"""
Script Maestro: Ejecuta todo el modelo NPS Relacional Sellers

Orden de ejecución:
1. Checkpoint 0: Carga de datos de encuestas sellers desde BigQuery
2. Checkpoint 2: Enriquecimiento con fuentes externas (Credits, Transacciones, Inversiones, Segmentación)
3. Checkpoint 1: Análisis de drivers NPS (motivos de encuestas) + dimensiones
4. Checkpoint 3 + 4 + 5: En paralelo (Tendencias, Alertas, Cualitativo)
5. HTML Final: Generación del resumen ejecutivo completo

USO:
    python ejecutar_modelo_completo.py
    
    O para forzar recarga de datos:
    python ejecutar_modelo_completo.py --recargar-datos

NOTA: Checkpoint 5 usa cache. Si ya existe el análisis para (site, mes),
      se salta automáticamente. Si no existe, se pausa y espera análisis de Claude.
"""

import os
import subprocess
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import argparse
import time
from datetime import timedelta
import json

# ==========================================
# IMPORTANTE: Forzar working directory al directorio del script
# ==========================================
script_dir = Path(__file__).parent.absolute()
os.chdir(str(script_dir))
print(f"📂 Working directory establecido: {script_dir}\n")

def cargar_tiempos_previos(data_dir: Path, site: str, mes_actual: str) -> dict:
    """Carga tiempos de ejecuciones previas si existen"""
    tiempos_path = data_dir / f'tiempos_ejecucion_{site}_{mes_actual}.json'
    if tiempos_path.exists():
        with open(tiempos_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def guardar_tiempos(data_dir: Path, site: str, mes_actual: str, tiempos: dict):
    """Guarda tiempos de ejecución para persistencia entre corridas"""
    tiempos_path = data_dir / f'tiempos_ejecucion_{site}_{mes_actual}.json'
    data_to_save = {
        "site": site,
        "mes": mes_actual,
        "tiempos": tiempos
    }
    with open(tiempos_path, 'w', encoding='utf-8') as f:
        json.dump(data_to_save, f, indent=2)

def ejecutar_script(script_path: Path, nombre: str):
    """Ejecuta un script de Python, maneja errores y mide tiempo"""
    print(f"\n{'='*80}")
    print(f"⏳ Ejecutando: {nombre}")
    print(f"{'='*80}\n")
    
    tiempo_inicio = time.time()
    
    try:
        env = {**os.environ, "PYTHONUTF8": "1"}
        result = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            cwd=str(script_path.parent.parent),
            env=env,
        )
        
        tiempo_fin = time.time()
        duracion = tiempo_fin - tiempo_inicio
        duracion_str = str(timedelta(seconds=int(duracion)))
        
        print(f"\n✅ Completado en: {duracion_str}")
        return True, duracion
        
    except subprocess.CalledProcessError as e:
        tiempo_fin = time.time()
        duracion = tiempo_fin - tiempo_inicio
        duracion_str = str(timedelta(seconds=int(duracion)))
        
        print(f"\n❌ Error en {nombre}")
        print(f"Código de salida: {e.returncode}")
        print(f"Tiempo antes del error: {duracion_str}")
        return False, duracion

def organizar_outputs(project_root: Path, site: str, mes_actual: str):
    """Organiza archivos: JSONs a data/, solo HTMLs en outputs/"""
    outputs_dir = project_root / 'outputs'
    data_dir = project_root / 'data'
    
    data_dir.mkdir(exist_ok=True)
    
    # Mover archivos de checkpoint 1 de outputs/ a data/ si existen ahí
    archivos_a_mover = [
        f'checkpoint1_consolidado_{site}_{mes_actual}.json',
    ]
    
    movidos = 0
    for archivo in archivos_a_mover:
        origen = outputs_dir / archivo
        if origen.exists():
            destino = data_dir / archivo
            shutil.move(str(origen), str(destino))
            movidos += 1
    
    # Limpiar archivos temporales en outputs
    for temp_file in outputs_dir.glob('.temp_*'):
        temp_file.unlink()
    
    # Limpiar archivos temporales de checkpoint 5 en data/
    # ONLY clean if CP5 succeeded (causas_raiz JSON exists)
    temp_eliminados = 0
    cp5_json = data_dir / f'checkpoint5_causas_raiz_{site}_{mes_actual}.json'
    if cp5_json.exists():
        for temp_file in data_dir.glob(f'temp_prompt_claude_{site}_{mes_actual}.*'):
            temp_file.unlink()
            temp_eliminados += 1
        for temp_file in data_dir.glob(f'temp_datos_preparados_{site}_{mes_actual}.*'):
            temp_file.unlink()
            temp_eliminados += 1
    
    print(f"   ✅ {movidos} archivos organizados en data/")
    print(f"   ✅ {temp_eliminados} archivos temporales eliminados")

def main():
    parser = argparse.ArgumentParser(description='Ejecuta el modelo NPS Relacional Sellers completo')
    parser.add_argument('--recargar-datos', action='store_true',
                       help='Elimina checkpoint0 y recarga datos desde BigQuery')
    args = parser.parse_args()
    
    project_root = Path(__file__).parent
    scripts_dir = project_root / 'scripts'
    outputs_dir = project_root / 'outputs'
    
    print("="*80)
    print("🚀 EJECUCIÓN COMPLETA DEL MODELO NPS RELACIONAL SELLERS")
    print("="*80)
    
    # Leer configuración
    import yaml
    config_path = project_root / 'config' / 'config.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)
    
    sites_config = config_data.get('sites', ['MLA'])
    site = sites_config[0] if sites_config else 'MLA'
    
    quarter_actual = config_data.get('quarter_actual', '26Q1')
    quarter_anterior = config_data.get('quarter_anterior', '25Q4')
    
    from src.nps_model.utils.dates import quarter_fecha_final, quarter_label
    mes_actual = quarter_fecha_final(quarter_actual)
    
    print(f"\n⚙️  Configuración:")
    print(f"   📍 Site: {site}")
    print(f"   📅 Comparación: {quarter_label(quarter_anterior)} vs {quarter_label(quarter_actual)}")
    
    # Si se solicita recargar datos, eliminar checkpoint0 y tiempos
    if args.recargar_datos:
        print(f"\n🗑️  Eliminando checkpoint0 para forzar recarga de datos...")
        data_dir = project_root / 'data'
        archivos_a_eliminar = [
            data_dir / f'datos_nps_{site}_{mes_actual}.parquet',
            data_dir / f'checkpoint0_{site}_{mes_actual}_metadatos.json',
            data_dir / f'tiempos_ejecucion_{site}_{mes_actual}.json'
        ]
        
        for archivo in archivos_a_eliminar:
            if archivo.exists():
                archivo.unlink()
                print(f"   ✅ Eliminado: {archivo.name}")
    
    # --- Fase 1: Secuencial (CP0 → CP2 → CP1) ---
    scripts_secuenciales = [
        (scripts_dir / 'test_checkpoint0_cargar_datos.py', 'Checkpoint 0: Carga de Datos Sellers'),
        (scripts_dir / 'test_checkpoint2_enriquecer_datos.py', 'Checkpoint 2: Enriquecimiento de Datos'),
        (scripts_dir / 'test_checkpoint1_drivers_nps.py', 'Checkpoint 1: Drivers NPS (Motivos)'),
    ]
    
    # --- Fase 2: Paralelo (CP3 + CP4 + CP5) ---
    scripts_paralelos = [
        (scripts_dir / 'test_checkpoint3_tendencias_anomalias.py', 'Checkpoint 3: Tendencias y Anomalías'),
        (scripts_dir / 'test_checkpoint4_alertas_emergentes.py', 'Checkpoint 4: Alertas Emergentes'),
        (scripts_dir / 'test_checkpoint5_analisis_cualitativo.py', 'Checkpoint 5: Análisis Cualitativo (Claude)'),
    ]
    
    # --- Fase 3: Secuencial (HTML) ---
    script_html = (scripts_dir / 'generar_html_final.py', 'Generación HTML Final')
    
    data_dir = project_root / 'data'
    
    tiempos_ejecucion = []
    tiempos_actualizados = {}
    tiempo_total_inicio = time.time()
    paso_num = 0
    
    # FASE 1: Ejecutar CP0, CP2, CP1 en secuencia
    for script_path, nombre in scripts_secuenciales:
        paso_num += 1
        exito, duracion = ejecutar_script(script_path, f"{paso_num}. {nombre}")
        tiempos_ejecucion.append((nombre, duracion))
        tiempos_actualizados[nombre] = duracion
        
        if not exito:
            guardar_tiempos(data_dir, site, mes_actual, tiempos_actualizados)
            print(f"\n{'='*80}")
            print(f"⏸️  EJECUCIÓN PAUSADA en paso {paso_num}")
            print(f"{'='*80}")
            sys.exit(1)
    
    # FASE 2: Ejecutar CP3, CP4, CP5 en paralelo
    print(f"\n{'='*80}")
    print(f"⚡ Ejecutando Checkpoints 3, 4 y 5 en paralelo...")
    print(f"{'='*80}\n")
    
    tiempo_paralelo_inicio = time.time()
    resultados_paralelos = {}
    
    def _run_parallel_script(script_path, nombre):
        """Wrapper para ejecutar un script y capturar resultado."""
        t0 = time.time()
        try:
            env = {**os.environ, "PYTHONUTF8": "1"}
            subprocess.run(
                [sys.executable, str(script_path)],
                check=True,
                cwd=str(script_path.parent.parent),
                env=env,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
            )
            return nombre, True, time.time() - t0, ""
        except subprocess.CalledProcessError as e:
            return nombre, False, time.time() - t0, (e.stdout or "") + (e.stderr or "")
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_run_parallel_script, sp, nm): nm
            for sp, nm in scripts_paralelos
        }
        for future in as_completed(futures):
            nombre_result, exito, dur, output = future.result()
            resultados_paralelos[nombre_result] = (exito, dur, output)
    
    tiempo_paralelo_total = time.time() - tiempo_paralelo_inicio
    
    cp5_failed = False
    for script_path, nombre in scripts_paralelos:
        paso_num += 1
        exito, dur, output = resultados_paralelos[nombre]
        tiempos_ejecucion.append((nombre, dur))
        tiempos_actualizados[nombre] = dur
        
        status = "✅" if exito else "❌"
        dur_str = str(timedelta(seconds=int(dur)))
        print(f"   {status} {nombre}: {dur_str}")
        
        if not exito and "Checkpoint 5" in nombre:
            cp5_failed = True
            cp5_output = output
    
    print(f"\n   ⏱️  Fase paralela completada en: {str(timedelta(seconds=int(tiempo_paralelo_total)))}")
    
    if cp5_failed:
        # CP5 es OBLIGATORIO — no generar HTML incompleto
        guardar_tiempos(data_dir, site, mes_actual, tiempos_actualizados)
        print(f"\n   ⚠️  CP5 sin cache — NO se generará HTML (análisis cualitativo es obligatorio)")
        print(f"   📋 Claude Code debe generar CP5 y re-ejecutar el modelo")
        # Signal to caller that CP5 needs generation
        prompt_path = data_dir / f"temp_prompt_claude_{site}_{mes_actual}.txt"
        if prompt_path.exists():
            print(f"\n⚠️  CP5_NEEDS_GENERATION: {prompt_path}")
            print(f"🎯 Claude Code debe generar CP5 y re-ejecutar.")
        sys.exit(5)

    # Check for other parallel failures (skip CP5 which is handled above)
    for script_path, nombre in scripts_paralelos:
        if "Checkpoint 5" in nombre:
            continue
        exito, dur, output = resultados_paralelos[nombre]
        if not exito:
            guardar_tiempos(data_dir, site, mes_actual, tiempos_actualizados)
            print(f"\n{'='*80}")
            print(f"❌ Error en {nombre}")
            if output:
                print(output[-500:])
            print(f"{'='*80}")
            sys.exit(1)

    # FASE 3: HTML Final (solo si CP5 existe)
    paso_num += 1
    exito, duracion = ejecutar_script(script_html[0], f"{paso_num}. {script_html[1]}")
    tiempos_ejecucion.append((script_html[1], duracion))
    tiempos_actualizados[script_html[1]] = duracion
    
    if not exito:
        guardar_tiempos(data_dir, site, mes_actual, tiempos_actualizados)
        print(f"\n{'='*80}")
        print(f"❌ Error generando HTML Final")
        print(f"{'='*80}")
        sys.exit(1)
    
    tiempo_total_real = time.time() - tiempo_total_inicio
    
    # Guardar tiempos actualizados
    guardar_tiempos(data_dir, site, mes_actual, tiempos_actualizados)
    
    tiempo_total_str = str(timedelta(seconds=int(tiempo_total_real)))
    
    # Éxito
    print(f"\n{'='*80}")
    print("✅ MODELO COMPLETO EJECUTADO EXITOSAMENTE")
    print(f"{'='*80}")
    
    # Organizar archivos
    print(f"\n🗂️  Organizando archivos...")
    organizar_outputs(project_root, site, mes_actual)
    
    print(f"\n📄 Resultado final:")
    print(f"   📊 HTML Principal:")
    print(f"      outputs/NPSRelSellers_{site}_{mes_actual}_[timestamp].html")
    print(f"\n   📦 Datos intermedios (en data/):")
    print(f"      • Checkpoints JSON (1, 3, 4, 5)")
    print(f"      • Datos Parquet (Encuestas Sellers)")
    print(f"      • Metadatos checkpoint0")
    print(f"\n   🧹 Limpieza automática:")
    print(f"      • Archivos temporales eliminados")
    print(f"      • Prompts de Claude eliminados")
    
    # Mostrar resumen de tiempos
    print(f"\n⏱️  Tiempos de ejecución:")
    print(f"{'='*80}")
    for nombre, duracion in tiempos_ejecucion:
        mins, secs = divmod(int(duracion), 60)
        if mins > 0:
            tiempo_str = f"{mins}min {secs}s"
        else:
            tiempo_str = f"{secs}s"
        print(f"   • {nombre}: {tiempo_str}")
    print(f"{'='*80}")
    print(f"   ⏱️  TIEMPO TOTAL: {tiempo_total_str}")
    print(f"{'='*80}")
    
    print(f"\n💡 Datos intermedios guardados en: data/")

    if cp5_failed:
        print(f"\n⚠️  CP5_NEEDS_GENERATION: data/temp_prompt_claude_{site}_{mes_actual}.txt")
        print(f"🎯 HTML generado sin Tab 4. Claude Code debe generar CP5 y re-ejecutar.")
        print(f"{'='*80}\n")
        sys.exit(5)  # Exit code 5 = CP5 needs generation
    else:
        print(f"\n🎉 ¡Resumen ejecutivo listo para presentar!")
        print(f"{'='*80}\n")

if __name__ == '__main__':
    main()
