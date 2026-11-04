#!/usr/bin/env python3
"""Genera docs/demostracion_documentada.md: la demostración de la sección 4 de los entregables, en formato escrito.
Cada punto pedido por el enunciado lleva: qué se hace, cómo se repite y un EXTRACTO REAL de la evidencia correspondiente
(se copia de los archivos de evidencias/, no se escribe a mano). No necesita AWS.
Uso (desde la raíz del repositorio):  python3 infra/generar_demostracion.py        (detecta si es el data lake o el warehouse)
"""
import os
import re
import sys

LAKE = {
    "titulo": "Demostración documentada del data lake de noticias",
    "intro": ("Este documento recorre, punto por punto, la demostración que pide la sección 4 de los entregables del data lake. "
              "Es una demostración **escrita**: cada paso explica qué se ejecuta y cómo repetirlo, y muestra un extracto real "
              "de la evidencia generada al ejecutarlo (los archivos completos están en `evidencias/`)."),
    "secciones": [
        {"titulo": "1. Ejecutar una ingesta completa",
         "texto": ("Se dispara el DAG `datalake_noticias` en Airflow (EC2). Corre sin intervención entre etapas: valida la configuración y el acceso "
                   "a S3, Glue, Athena y Comprehend; extrae los tres feeds RSS de El Tiempo a **Bronze** (XML original sin cambios, con un manifiesto que "
                   "guarda el hash SHA-256, el estado HTTP y la hora UTC); limpia, valida y deduplica en **Silver** (los registros defectuosos van a "
                   "cuarentena con su motivo); enriquece con **Comprehend** (entidades y frases clave, solo si el texto cambió); construye **Gold**; registra "
                   "tablas y particiones en **Glue**; ejecuta 12 validaciones con **Athena** y registra el estado del lote.\n\n"
                   "Cada fuente queda con un estado explícito (`OK`, `FEED_SIN_NOTICIAS`, `ERROR_HTTP`, `ERROR_DE_RED`, `RESPUESTA_VACIA`, "
                   "`XML_MALFORMADO`) y el lote queda `COMPLETA`, `PARCIAL` o `FALLIDA`: una fuente caída no se confunde con una fuente sin noticias."),
         "comando": 'ssh -i ~/.ssh/taller-datalake-key.pem ec2-user@$EC2_IP "cd ~/DE-DataLake/infra/airflow && docker compose exec -T airflow-scheduler airflow dags trigger datalake_noticias"',
         "evidencias": [("evidencias/airflow_evidencia.txt", "== 1.", "== 2.", 30, "Tareas del DAG y sus dependencias"),
                        ("evidencias/bronze/bronze_evidencia.txt", "=================== 3.", "=================== 4.", 30, "Resumen del lote en Bronze"),
                        ("evidencias/bronze/bronze_evidencia.txt", "=================== 5.", "=================== 6.", 12, "Integridad: el hash del manifiesto coincide con el archivo guardado"),
                        ("evidencias/escenarios/01_carga_inicial.txt", None, None, 40, "Escenario de carga inicial")]},
        {"titulo": "2. Consultar los indicadores en Athena",
         "texto": ("Las tablas de Gold (`gold_noticias_por_dia`, `gold_entidades_por_dia`, `gold_frases_por_seccion_dia`) responden las cuatro preguntas del enunciado: "
                   "noticias por día, medio y sección; personas, organizaciones y lugares más frecuentes; frases clave por sección; y cómo cambia la presencia de "
                   "una entidad entre fechas. Cuentan **noticias distintas**, no menciones repetidas. Las tablas están particionadas por fecha de publicación, así que una "
                   "consulta con filtro de partición escanea muchos menos bytes que la misma sin filtro (ver la comparación al final del extracto)."),
         "comando": "python3 infra/consultas_athena.py   # ejecuta sql/consultas_athena.sql y escribe evidencias/athena_evidencia.txt",
         "evidencias": [("evidencias/athena_evidencia.txt", None, None, 55, "Consultas y resultados"),
                        ("evidencias/athena_evidencia.txt", "=== COMPARACIÓN DE BYTES", None, 16, "Bytes escaneados con y sin filtro de partición")]},
        {"titulo": "3. Reprocesar un lote sin duplicar resultados",
         "texto": ("Se vuelve a ejecutar el DAG con el parámetro `reprocess_run_id = <lote de Bronze>`. En ese modo **no se consultan los RSS**: se leen los archivos "
                   "que ya están en Bronze. Silver reconoce todas las noticias como repetidas, Comprehend no se vuelve a llamar (el texto no cambió), y Gold y Glue no "
                   "cambian ninguna partición, porque cada partición se publica en una carpeta versionada y solo se confirma si su contenido cambió. "
                   "Además, borrar Silver y Gold y reconstruirlos desde Bronze produce exactamente el mismo resultado: la huella (identificadores más hash de contenido) coincide."),
         "comando": "python3 infra/escenarios.py repeticion    # reprocesa un lote ya exitoso y compara el estado antes y después",
         "evidencias": [("evidencias/escenarios/03_repeticion_lote_exitoso.txt", None, None, 40, "Repetición de un lote exitoso"),
                        ("evidencias/escenarios/reproceso_bronze_despues.txt", None, None, 20, "Reconstrucción completa desde Bronze: la huella coincide")]},
        {"titulo": "4. Recuperar una ejecución después de un fallo controlado",
         "texto": ("El parámetro `simular_fallo` provoca **un** fallo controlado por lote y etapa. Con `nlp_a_mitad`, Comprehend falla después de analizar y guardar la primera tanda: "
                   "el reintento de Airflow solo analiza las noticias que faltaban, sin repetir llamadas ni duplicar resultados. Con `gold_publicacion`, los archivos de Gold se escriben "
                   "pero no se confirman: Athena sigue viendo la versión anterior completa hasta que el reintento termina. En ambos casos los registros llevan lote, tarea y fuente, "
                   "y el estado del procesamiento queda en `runs/dag_run=<ejecución>/pipeline_status.json`."),
         "comando": "python3 infra/escenarios.py fallo_nlp fallo_gold",
         "evidencias": [("evidencias/escenarios/06a_fallo_comprehend.txt", None, None, 45, "Fallo a mitad de Comprehend: intento 1 falla, intento 2 completa"),
                        ("evidencias/escenarios/06b_fallo_publicacion_gold.txt", None, None, 45, "Fallo al publicar Gold")]},
        {"titulo": "5. Una limitación del enriquecimiento de texto",
         "texto": ("Amazon Comprehend detecta **menciones**, no entiende el contexto. Por ejemplo, «Ministro de Justicia» salió clasificado como `PERSON` con una confianza de 0,97, "
                   "aunque es un cargo y no una persona; una puntuación alta no corrige el error. Tampoco resuelve alias («Fiscalía» y «Fiscalía General» son entidades distintas), "
                   "las frases clave pueden salir vacías en textos formados solo por nombres propios, y los textos se recortan a 4.900 bytes (estado `ANALIZADO_TRUNCADO`). "
                   "Por eso el análisis es una ayuda para explorar, no una verdad."),
         "comando": "sed -n '/== 6/,$p' evidencias/comprehend_evidencia.txt",
         "evidencias": [("evidencias/comprehend_evidencia.txt", "== 6.", None, 22, "Cargos clasificados como personas y otras limitaciones")]},
    ],
    "cierre": [("evidencias/escenarios/RESUMEN.txt", None, None, 14, "Resultado de todos los escenarios")],
}

DW = {
    "titulo": "Demostración documentada del data warehouse en AWS",
    "intro": ("Este documento recorre la demostración que pide la sección 4 de los entregables del data warehouse. Es una demostración **escrita**: explica qué se ejecuta, "
              "cómo repetirlo y muestra extractos reales de las evidencias generadas al ejecutarlo (los archivos completos están en `evidencias/dw/` y `evidencias/escenarios_dw/`).\n\n"
              "**Alcance:** QuickSight quedó fuera por indicación del profesor, así que «hasta el dashboard» se entiende como hasta las tablas del modelo analítico (`marts`) "
              "y la conciliación de un indicador con su consulta SQL."),
    "secciones": [
        {"titulo": "1. Procesar un nuevo lote desde las fuentes hasta el modelo analítico",
         "texto": ("Se agrega un lote diario a las fuentes (3 pedidos, 3 reseñas y 3 eventos en RDS MySQL y DocumentDB) y se ejecuta el DAG `dw_pipeline`, de 7 pasos y sin intervención manual: "
                   "(1) validar conexiones y parámetros; (2) extraer de forma **incremental** desde la marca de avance, más una ventana de 7 días para datos tardíos; (3) escribir el lote en S3 "
                   "(`raw/<fuente>/<entidad>/batch_id=<id>/`, con manifiesto, sin sobrescribir); (4) cargar en Redshift con `COPY`, verificando los conteos; (5) transformar con dbt "
                   "(staging, calidad con cuarentena, snapshots SCD2 y marts); (6) pruebas de calidad y conciliación; (7) registrar el resultado. Los hechos crecen exactamente con lo nuevo, "
                   "y un indicador (ventas y líneas por mes) se concilia entre el origen válido y el modelo con una consulta SQL."),
         "comando": "./infra/agregar_lote_dw.sh 40\n./infra/ejecutar_pipeline_dw.sh\n./infra/consulta_dw.sh --resumen\npython3 infra/evidencias_dw.py indicador",
         "evidencias": [("evidencias/escenarios_dw/04_nuevo_lote_incremental.txt", None, None, 50, "Escenario: nuevo lote incremental, antes y después"),
                        ("evidencias/dw/airflow_evidencia.txt", "== 3.", "== 4.", 28, "Estado de una ejecución exitosa: las tareas y las filas cargadas"),
                        ("evidencias/dw/redshift_estado.txt", "== Hechos", "== Dimensiones", 10, "Hechos en Redshift"),
                        ("evidencias/dw/indicador_ventas.txt", "== Conciliación", None, 12, "Conciliación del indicador entre el origen válido y el modelo")]},
        {"titulo": "2. Cómo se identifica y se recupera una ejecución fallida",
         "texto": ("**Identificación.** Cada lote tiene un identificador (`AAAAMMDDTHHMMSSZ`) y un estado en `control.batches`: `INICIADO`, `EN_S3`, `CARGADO`, `TRANSFORMADO`, `COMPLETO` o `FALLIDO`. "
                   "Una ejecución fallida se ve en la interfaz de Airflow (la tarea en rojo), en `runs/dag_run=<ejecución>/pipeline_status.json` en S3 (estado de cada tarea), "
                   "en `control.batches` (estado `FALLIDO` y detalle) y en los registros, cuyas líneas llevan `[lote=...] [tarea=...]`.\n\n"
                   "**Recuperación.** Airflow reintenta hasta 2 veces con el **mismo lote**. La carga a Redshift reemplaza las filas de ese lote en `raw` (no las duplica) y dbt reconstruye desde `raw`. "
                   "La marca de avance solo se mueve cuando el lote termina `COMPLETO`, después de las pruebas: si algo falla antes, el siguiente lote vuelve a pedir la misma ventana y no se pierde nada. "
                   "Si no se recupera solo, se reejecuta el mismo lote o se reprocesa desde S3 con `reprocess_batch_id`, sin consultar las fuentes."),
         "comando": "./infra/ejecutar_pipeline_dw.sh '{\"simular_fallo\": \"carga_redshift\"}'    # fallo controlado: el primer intento falla y el segundo completa",
         "evidencias": [("evidencias/escenarios_dw/06a_fallo_carga_redshift.txt", None, None, 50, "Fallo controlado en la carga a Redshift"),
                        ("evidencias/escenarios_dw/06b_fallo_dbt_marts.txt", None, None, 50, "Fallo controlado en la transformación con dbt"),
                        ("evidencias/dw/redshift_estado.txt", "== Lotes registrados", "== Marcas de avance", 10, "Estados de los lotes en control.batches (incluye lotes FALLIDO de las primeras corridas)")]},
        {"titulo": "3. Otros escenarios validados",
         "texto": ("Además de lo pedido, se validaron la carga inicial, los registros defectuosos (cuarentena con motivo y lote de origen), la repetición de un lote exitoso (sin duplicar hechos ni versiones "
                   "históricas) y el cambio en un atributo historizado (SCD tipo 2: la versión anterior se cierra y los hechos posteriores apuntan a la nueva)."),
         "comando": "python3 infra/escenarios_dw.py",
         "evidencias": [("evidencias/escenarios_dw/RESUMEN.txt", None, None, 12, "Resultado de todos los escenarios")]},
    ],
    "cierre": [],
}


def enmascarar(texto):
    texto = re.sub(r"\b\d{12}\b", "************", texto)
    return re.sub(r"\b(?!127\.0\.0\.1\b)(?:\d{1,3}\.){3}\d{1,3}\b", "x.x.x.x", texto)


def extracto(ruta, desde, hasta, maximo):
    if not os.path.exists(ruta):
        return None
    lineas = open(ruta, encoding="utf-8").read().splitlines()
    inicio = next((i for i, l in enumerate(lineas) if desde and desde in l), 0) if desde else 0
    fin = next((i for i in range(inicio + 1, len(lineas)) if hasta and hasta in lineas[i]), len(lineas)) if hasta else len(lineas)
    trozo = lineas[inicio:fin]
    texto = "\n".join(trozo[:maximo]) + ("\n... (continúa en el archivo completo)" if len(trozo) > maximo else "")
    return enmascarar(texto)


def bloque(ruta, desde, hasta, maximo, descripcion, faltan):
    cuerpo = extracto(ruta, desde, hasta, maximo)
    if cuerpo is None:
        faltan.append(ruta)
        return f"**Evidencia:** {descripcion} — `{ruta}`\n\n> No se encontró el archivo. Ejecuta el paso indicado para generarlo.\n"
    return f"**Evidencia:** {descripcion} — `{ruta}`\n\n```text\n{cuerpo}\n```\n"


def main():
    proyecto = sys.argv[1] if len(sys.argv) > 1 else ("lake" if os.path.isdir("src/datalake") else "dw" if os.path.isdir("src/ecommerce_dw") else None)
    if proyecto not in ("lake", "dw"):
        sys.exit("Ejecútalo desde la raíz del repositorio del data lake o del warehouse (o indica: lake | dw).")
    spec = LAKE if proyecto == "lake" else DW
    faltan, partes = [], [f"# {spec['titulo']}\n", spec["intro"] + "\n"]
    for s in spec["secciones"]:
        partes += [f"## {s['titulo']}\n", s["texto"] + "\n", "**Cómo se repite:**\n", f"```bash\n{s['comando']}\n```\n"]
        partes += [bloque(r, d, h, m, desc, faltan) for r, d, h, m, desc in s["evidencias"]]
    if spec["cierre"]:
        partes += ["## Resultado de los escenarios\n"] + [bloque(r, d, h, m, desc, faltan) for r, d, h, m, desc in spec["cierre"]]
    os.makedirs("docs", exist_ok=True)
    with open("docs/demostracion_documentada.md", "w", encoding="utf-8") as f:
        f.write("\n".join(partes))
    print("Escrito docs/demostracion_documentada.md")
    if faltan:
        print("AVISO: no se encontraron estos archivos de evidencia (esas secciones quedaron con una nota):")
        for r in sorted(set(faltan)):
            print("  -", r)


if __name__ == "__main__":
    main()
