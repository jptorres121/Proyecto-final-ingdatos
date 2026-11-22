"""
Streaming con Apache Kafka y Spark Structured Streaming, aplicado a los datos de Spotify.

  1. Los registros de un dataset se convierten en eventos de reproducción (JSON).
  2. Kafka SIMULADO en Python (producer, topic con particiones, offsets, consumer group):
     no necesita instalar ni levantar un broker. Procesa en micro-lotes, descarta
     duplicados y mide latencia y throughput.
  3. Spark Structured Streaming REAL: esquema + readStream, filter (Ejemplo 1),
     groupBy + count con outputMode("complete") (Ejemplo 2), y watermark + deduplicación.

Se usa desde el menú principal de ingdatos.py (opción 6), o solo con: python streaming.py
"""

import json
import os
import random
import shutil
import tempfile
import time
import warnings
import zlib
from datetime import datetime, timedelta

import pandas as pd


_BASE_TS = datetime(2026, 10, 21, 14, 0, 0)

ESQUEMA_SPOTIFY = ("evento_id STRING, cancion STRING, artista STRING, genero STRING, "
                   "reproducciones LONG, evento_ts STRING")


# =============================================================================
# 1. EVENTOS DE SPOTIFY
# =============================================================================

def generar_eventos_spotify(df, buscar_columna, limpiar_artista, cantidad=30,
                            tasa_duplicados=0.1, semilla=42):
    """Convierte filas del dataset en eventos de reproducción. Para practicar la
    deduplicación se repiten a propósito algunos eventos (mismo evento_id), como pasa
    con los mensajes duplicados de un sistema real.

    Devuelve (lista_de_eventos, cantidad_de_duplicados_inyectados) o (None, 0) si el
    dataset no tiene columna de título."""
    col_titulo = buscar_columna(df, ["name", "track_name", "title", "titulo_track"])
    if not col_titulo:
        return None, 0
    col_artista = buscar_columna(df, ["artists", "artist", "artist_name", "nombre_artista"])
    col_genero = buscar_columna(df, ["genre", "track_genre", "genero_musical"])
    col_streams = buscar_columna(df, ["streams", "playcount", "cantidad_reproducciones"])

    muestra = df.dropna(subset=[col_titulo]).sample(n=min(cantidad, len(df)), random_state=semilla)
    eventos = []
    for i, (_, fila) in enumerate(muestra.iterrows(), start=1):
        reproducciones = 1
        if col_streams is not None and pd.notna(fila[col_streams]):
            try:
                reproducciones = int(fila[col_streams])
            except (TypeError, ValueError):
                reproducciones = 1
        eventos.append({
            "evento_id": f"EV-{i:04d}",
            "cancion": str(fila[col_titulo]),
            "artista": str(limpiar_artista(fila[col_artista])) if col_artista and pd.notna(fila[col_artista]) else "desconocido",
            "genero": str(fila[col_genero]) if col_genero and pd.notna(fila[col_genero]) else "sin_genero",
            "reproducciones": reproducciones,
            "evento_ts": (_BASE_TS + timedelta(seconds=20 * i)).isoformat(),
        })

    rnd = random.Random(semilla)
    n_dup = int(round(len(eventos) * tasa_duplicados))
    for original in rnd.sample(eventos, n_dup):
        eventos.insert(eventos.index(original) + rnd.randint(1, 3), dict(original))
    return eventos, n_dup


def _umbral_alerta(eventos):
    """Umbral = percentil 90 de las reproducciones de los eventos."""
    valores = pd.Series([e["reproducciones"] for e in eventos])
    return float(valores.quantile(0.9))


# =============================================================================
# 2. KAFKA SIMULADO (productor -> tópico con particiones -> consumidor)
# =============================================================================

class Topico:
    """Tópico de Kafka simulado: lista de particiones; cada evento tiene un offset."""

    def __init__(self, nombre, particiones=3):
        self.nombre = nombre
        self.particiones = [[] for _ in range(particiones)]   # cada registro: (evento, instante_publicado)
        self._offsets_grupo = {}   # grupo -> siguiente offset a leer, por partición
        self._siguiente_rr = 0

    def publicar(self, evento, clave=None):
        """Agrega el evento a una partición. Con clave, la misma clave va siempre a la misma
        partición (así se conserva el orden de esa clave); sin clave, se reparte por turnos."""
        if clave is not None:
            p = zlib.crc32(str(clave).encode("utf-8")) % len(self.particiones)
        else:
            p = self._siguiente_rr % len(self.particiones)
            self._siguiente_rr += 1
        self.particiones[p].append((evento, time.perf_counter()))
        return p, len(self.particiones[p]) - 1

    def total_eventos(self):
        return sum(len(p) for p in self.particiones)

    def offsets(self, grupo):
        return list(self._offsets_grupo.get(grupo, [0] * len(self.particiones)))


class Productor:
    """Aplicación que publica eventos en un tópico."""

    def __init__(self, topico):
        self.topico = topico

    def enviar(self, evento, clave=None):
        return self.topico.publicar(evento, clave)


class Consumidor:
    """Lee eventos de un tópico. Los consumidores del mismo grupo comparten la posición
    (offset), así que no leen dos veces el mismo evento; un grupo distinto lee todo desde el inicio."""

    def __init__(self, topico, grupo, nombre="consumidor"):
        self.topico = topico
        self.grupo = grupo
        self.nombre = nombre
        topico._offsets_grupo.setdefault(grupo, [0] * len(topico.particiones))

    def leer(self, maximo=None):
        """Devuelve hasta 'maximo' eventos nuevos como lista de (partición, offset, evento, instante_publicado)."""
        offsets = self.topico._offsets_grupo[self.grupo]
        leidos = []
        for p, registros in enumerate(self.topico.particiones):
            while offsets[p] < len(registros):
                if maximo is not None and len(leidos) >= maximo:
                    return leidos
                evento, t_pub = registros[offsets[p]]
                leidos.append((p, offsets[p], evento, t_pub))
                offsets[p] += 1
        return leidos

    def pendientes(self):
        offsets = self.topico._offsets_grupo[self.grupo]
        return sum(len(r) - o for r, o in zip(self.topico.particiones, offsets))


def simular_spotify_con_kafka(eventos, tamano_microlote=10, espera_por_lote=0.05):
    """Procesa los eventos en micro-lotes desde un tópico Kafka simulado, con estado
    (total por artista), deduplicación y medición de latencia y throughput.

    espera_por_lote simula el tiempo de procesamiento de cada micro-lote."""
    topico = Topico("spotify.reproducciones", particiones=3)
    productor = Productor(topico)
    consumidor = Consumidor(topico, grupo="analitica-spotify")
    umbral = _umbral_alerta(eventos)

    print(f"\n===== Kafka simulado: flujo productor -> tópico -> consumidor =====")
    for e in eventos:
        productor.enviar(e, clave=e["artista"])
    print(f"[Producer] publicó {topico.total_eventos()} eventos en {len(topico.particiones)} particiones.")

    vistos = set()
    totales = {}
    duplicados = 0
    alertas = []
    latencias = []
    lote_n = 0
    inicio = time.perf_counter()
    print(f"\n[Consumer] grupo '{consumidor.grupo}', micro-lotes de {tamano_microlote} eventos. "
          f"Alerta si reproducciones > {umbral:,.0f}")
    while consumidor.pendientes() > 0:
        lote_n += 1
        lote = consumidor.leer(maximo=tamano_microlote)
        time.sleep(espera_por_lote)   # tiempo de procesamiento simulado
        ahora = time.perf_counter()
        nuevos = dup_lote = 0
        for _, _, e, t_pub in lote:
            latencias.append((ahora - t_pub) * 1000)
            if e["evento_id"] in vistos:   # deduplicación: mismo evento_id = mismo evento
                dup_lote += 1
                continue
            vistos.add(e["evento_id"])
            nuevos += 1
            totales[e["artista"]] = totales.get(e["artista"], 0) + e["reproducciones"]
            if e["reproducciones"] > umbral:
                alertas.append(e)
        duplicados += dup_lote
        print(f"  micro-lote {lote_n}: {len(lote)} leídos, {nuevos} nuevos, {dup_lote} duplicados descartados "
              f"| artistas en el estado: {len(totales)}")
    duracion = time.perf_counter() - inicio

    print("\n[Salida] Top 5 artistas por reproducciones (estado final):")
    for artista, total in sorted(totales.items(), key=lambda kv: kv[1], reverse=True)[:5]:
        print(f"  {artista[:35]:<35} {total:>15,}")
    print(f"\nAlertas (reproducciones > {umbral:,.0f}): {len(alertas)}")
    for a in alertas[:5]:
        print(f"  {a['evento_id']} {a['cancion'][:30]} - {a['artista'][:25]}: {a['reproducciones']:,}")

    print("\n[Métricas del sistema]")
    print(f"  Eventos leídos: {len(latencias)} | únicos: {len(vistos)} | duplicados descartados: {duplicados}")
    print(f"  Latencia (publicado -> procesado): promedio {sum(latencias) / len(latencias):.1f} ms, "
          f"máxima {max(latencias):.1f} ms")
    print(f"  Throughput: {len(latencias) / duracion:,.0f} eventos/segundo")
    print("  (la latencia crece en los últimos micro-lotes porque esos eventos esperaron en el tópico;")
    print("   en la simulación el tiempo de procesamiento es una pausa fija, no una medida de Kafka real)")


# =============================================================================
# 3. SPARK STRUCTURED STREAMING (REAL)
# =============================================================================

def _escribir_lote(carpeta, numero, eventos):
    """Deja un archivo JSON (un evento por línea) en la carpeta que Spark está observando."""
    ruta = os.path.join(carpeta, f"lote_{numero:03d}.json")
    with open(ruta, "w", encoding="utf-8") as f:
        for e in eventos:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def _dividir(lista, partes):
    tam = max(1, -(-len(lista) // partes))
    return [lista[i:i + tam] for i in range(0, len(lista), tam)]


def _ejecutar_spark(titulo, lotes, esquema, consultas):
    """Ejecuta consultas de Structured Streaming sobre una carpeta de entrada.

    lotes:     lista de listas de eventos; cada una se escribe como un archivo JSON NUEVO
               para simular la llegada continua de datos.
    esquema:   esquema declarado (en lectura de archivos no se infiere).
    consultas: lista de (nombre, funcion(DataFrame_streaming) -> DataFrame, modo_salida).

    En clase el destino era la consola con awaitTermination(); aquí se usa un 'memory sink'
    (tabla en memoria) y processAllAvailable() para poder mostrar el resultado tras cada lote."""
    # PySpark avisa si pandas es muy nuevo; no afecta esta demo (no se usa toPandas)
    warnings.filterwarnings("ignore", message=".*PySpark does not yet fully support pandas.*")
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("\nFalta PySpark. Instálalo con: pip install pyspark")
        print("(Spark también necesita Java instalado.)")
        return False

    carpeta = tempfile.mkdtemp(prefix="stream_entrada_")
    spark = None
    activas = []
    try:
        print("\nIniciando Spark (la primera vez puede tardar unos segundos)...")
        spark = (SparkSession.builder.appName(titulo).master("local[2]")
                 .config("spark.sql.shuffle.partitions", "2")
                 .config("spark.ui.enabled", "false")
                 .config("spark.ui.showConsoleProgress", "false")
                 .config("spark.driver.host", "127.0.0.1")
                 .config("spark.driver.bindAddress", "127.0.0.1")
                 .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")

        # readStream: fuente que puede seguir recibiendo archivos nuevos
        eventos = spark.readStream.schema(esquema).json(carpeta)

        for nombre, construir, modo in consultas:
            resultado = construir(eventos)
            q = (resultado.writeStream.format("memory").queryName(nombre)
                 .outputMode(modo).start())
            activas.append(q)
        print(f"{len(activas)} consultas de streaming activas sobre la carpeta de entrada.")

        for numero, lote in enumerate(lotes, start=1):
            _escribir_lote(carpeta, numero, lote)
            for q in activas:
                q.processAllAvailable()   # espera a que Spark procese lo que llegó
            print(f"\n----- Llegó el archivo {numero} de {len(lotes)} ({len(lote)} eventos) -----")
            for nombre, _, modo in consultas:
                # collect() en vez de toPandas(): no depende de PyArrow ni de la versión de pandas
                filas = [r.asDict() for r in spark.table(nombre).collect()]
                tabla = pd.DataFrame(filas)
                if len(tabla):
                    tabla = tabla.sort_values(list(tabla.columns)[0]).reset_index(drop=True)
                print(f"\n[{nombre}]  (modo de salida: {modo}, {len(tabla)} filas)")
                print(tabla.to_string(index=False) if len(tabla) else "  (sin resultados todavía)")
        exito = True
    except Exception as e:
        exito = False
        primera = (str(e).strip().splitlines() or [""])[0][:200]
        print(f"\nNo se pudo ejecutar Spark: {primera}")
    finally:
        for q in activas:
            try:
                q.stop()
            except Exception:
                pass
        if spark is not None:
            spark.stop()   # libera la JVM y su memoria
        shutil.rmtree(carpeta, ignore_errors=True)
    return exito


def _spark_listo():
    """(ok, motivo): ¿puede arrancar Spark aquí? En Windows exige HADOOP_HOME con winutils."""
    try:
        import pipeline_avanzado
        return pipeline_avanzado._spark_disponible()
    except Exception as e:
        return False, str(e)


def _emular_con_pandas(lotes, umbral):
    """Mismas 3 consultas de Structured Streaming, calculadas con Pandas lote a lote (resultado inmediato)."""
    vistos = []
    for numero, lote in enumerate(lotes, start=1):
        vistos.extend(lote)
        acum = pd.DataFrame(vistos)
        print(f"\n----- Llegó el archivo {numero} de {len(lotes)} ({len(lote)} eventos) -----")
        destacados = acum[acum["reproducciones"] > umbral]
        t1 = destacados[["evento_id", "cancion", "artista", "genero", "reproducciones", "evento_ts"]]
        t2 = (destacados.groupby("artista").size().rename("eventos_destacados").reset_index()
              .sort_values("artista"))
        unicos = acum.drop_duplicates(["evento_id", "evento_ts"])      # dropDuplicates con watermark
        t3 = (unicos.groupby("artista").agg(total_reproducciones=("reproducciones", "sum"),
                                            eventos_unicos=("evento_id", "count")).reset_index())
        for nombre, tabla, modo in [("ejemplo1_filtro", t1, "append"),
                                    ("ejemplo2_conteo_por_artista", t2, "complete"),
                                    ("extra_total_por_artista_sin_duplicados", t3, "complete")]:
            print(f"\n[{nombre}]  (modo de salida: {modo}, {len(tabla)} filas)")
            print(tabla.to_string(index=False) if len(tabla) else "  (sin resultados todavía)")


def spark_streaming_spotify(eventos):
    """Ejemplos de clase con los eventos de Spotify:
       Ejemplo 1: leer eventos de una carpeta y filtrar.
       Ejemplo 2: agregado continuo (groupBy + count) con outputMode("complete").
       Extra:     watermark + deduplicación para controlar duplicados y estado."""
    umbral = _umbral_alerta(eventos)

    def ejemplo1_filtro(e):
        from pyspark.sql.functions import col
        return e.filter(col("reproducciones") > umbral)

    def ejemplo2_conteo_por_artista(e):
        from pyspark.sql.functions import col, count
        return (e.filter(col("reproducciones") > umbral)
                .groupBy("artista").agg(count("*").alias("eventos_destacados")))

    def _sin_duplicados(e):
        from pyspark.sql.functions import to_timestamp
        # watermark: Spark solo conserva estado de deduplicación de los últimos 10 minutos de evento
        return (e.withColumn("ts", to_timestamp("evento_ts"))
                .withWatermark("ts", "10 minutes")
                .dropDuplicates(["evento_id", "ts"]))

    def extra_total_por_artista_sin_duplicados(e):
        from pyspark.sql.functions import count, sum as sum_
        return (_sin_duplicados(e).groupBy("artista")
                .agg(sum_("reproducciones").alias("total_reproducciones"),
                     count("*").alias("eventos_unicos")))

    consultas = [
        ("ejemplo1_filtro", ejemplo1_filtro, "append"),
        ("ejemplo2_conteo_por_artista", ejemplo2_conteo_por_artista, "complete"),
        ("extra_total_por_artista_sin_duplicados", extra_total_por_artista_sin_duplicados, "complete"),
    ]
    print("\n===== Spark Structured Streaming con eventos de Spotify =====")
    print(f"{len(eventos)} eventos (con duplicados intencionales) llegan en 3 archivos JSON nuevos.")
    print("""
Pasos (como en clase):
  1. Definir esquema: campos y tipos; en lectura de archivos evita inferirlos.
  2. readStream:      fuente que puede seguir recibiendo archivos nuevos.
  3. Transformar:     filter / groupBy, igual que con un DataFrame tradicional.
  4. writeStream:     destino y modo de salida; start() inicia la consulta.""")
    print(f"""
Consultas:
  ejemplo1_filtro          -> filter(reproducciones > {umbral:,.0f}), modo append
  ejemplo2_conteo_por_artista -> groupBy('artista').count() sobre los filtrados, modo complete
  extra_total_por_artista_sin_duplicados -> withWatermark + dropDuplicates y suma por artista, modo complete
  (los agregados mantienen estado; el watermark evita que el estado de deduplicación crezca sin fin)""")
    lotes = _dividir(eventos, 3)
    ok, motivo = _spark_listo()
    if ok:
        if _ejecutar_spark("SpotifyStreaming", lotes, ESQUEMA_SPOTIFY, consultas):
            return
        motivo = "Spark no pudo arrancar en este equipo (Hadoop/Java)."
    print(f"\n[Motor Pandas] {motivo}")
    print("Se calculan las mismas 3 consultas con Pandas, lote a lote (mismo resultado, inmediato).")
    _emular_con_pandas(lotes, umbral)


def leer_desde_kafka(spark, esquema, servidor="localhost:9092", topico="spotify.reproducciones"):
    """Fuente REAL de Kafka para Structured Streaming (alternativa a la carpeta de archivos).

    Requiere un broker Kafka en ejecución y el conector de Spark para Kafka
    (paquete spark-sql-kafka-0-10, con la versión que coincida con tu Spark y Scala).
    NO se usa desde el menú porque necesita ese entorno; sirve de plantilla:

        eventos = leer_desde_kafka(spark, ESQUEMA_SPOTIFY)
        # el resto del código (filter, groupBy, writeStream) es idéntico al de archivos
    """
    from pyspark.sql.functions import col, from_json
    crudo = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", servidor)
             .option("subscribe", topico)
             .option("startingOffsets", "earliest")
             .load())
    return (crudo.select(from_json(col("value").cast("string"), esquema).alias("e"))
            .select("e.*"))


# =============================================================================
# 4. MENÚ
# =============================================================================

def _dataframe_ejemplo(semilla=7):
    """Datos INVENTADOS de prueba (no son de Spotify) para ver el módulo sin descargar nada."""
    rnd = random.Random(semilla)
    n = 60
    return pd.DataFrame({
        "name": [f"Cancion {i}" for i in range(1, n + 1)],
        "artists": [f"['Artista {i % 8 + 1}']" for i in range(n)],
        "genre": [rnd.choice(["pop", "rock", "salsa", "reggaeton"]) for _ in range(n)],
        "streams": [rnd.randint(10_000, 900_000) for _ in range(n)],
    })


def _pedir_eventos(cargar_dataframe, buscar_columna, limpiar_artista, datasets):
    """Pregunta de qué dataset sacar los eventos y los genera. Devuelve eventos o None."""
    print("\n¿De dónde quieres tomar los eventos?")
    for clave, info in (datasets or {}).items():
        print(f"  {clave}. {info['nombre']}")
    print("  0. Datos de ejemplo inventados (sin descargar nada)")
    sel = input("Selecciona (o 'c' para cancelar): ").strip()

    if sel == "0":
        df, nombre = _dataframe_ejemplo(), "datos de ejemplo inventados"
        bc = buscar_columna or (lambda d, cands: next((c for c in d.columns if c.lower() in [x.lower() for x in cands]), None))
        la = limpiar_artista or (lambda v: str(v).strip("[]").strip("'"))
    elif datasets and sel in datasets and cargar_dataframe is not None:
        df, nombre = cargar_dataframe(sel)
        bc, la = buscar_columna, limpiar_artista
        if df is None:
            return None
    else:
        return None

    eventos, n_dup = generar_eventos_spotify(df, bc, la)
    if eventos is None:
        print("Ese dataset no tiene una columna de título de canción; prueba con otro.")
        return None
    print(f"\nSe generaron {len(eventos)} eventos desde '{nombre}' "
          f"({n_dup} duplicados inyectados a propósito).")
    return eventos


def menu_streaming(cargar_dataframe=None, buscar_columna=None, limpiar_artista=None, datasets=None):
    """Menú de streaming. Los parámetros permiten usar los datasets del proyecto."""
    eventos = None

    while True:
        print("\n=== STREAMING EN TIEMPO REAL (Kafka + Spark Structured Streaming) ===")
        print("1. Streaming con Kafka simulado (productor -> tópico -> consumidor)")
        print("2. Spark Structured Streaming (requiere PySpark y Java)")
        print("3. Cambiar el origen de los eventos")
        print("4. Volver al menú principal")
        op = input("Selecciona una opción: ").strip()

        if op in ("1", "2"):
            if eventos is None:
                eventos = _pedir_eventos(cargar_dataframe, buscar_columna, limpiar_artista, datasets)
            if eventos is None:
                continue
            if op == "1":
                simular_spotify_con_kafka(eventos)
            else:
                spark_streaming_spotify(eventos)
        elif op == "3":
            nuevos = _pedir_eventos(cargar_dataframe, buscar_columna, limpiar_artista, datasets)
            if nuevos is not None:
                eventos = nuevos
        elif op == "4":
            break
        else:
            print("Opción inválida.")


if __name__ == "__main__":
    menu_streaming()
