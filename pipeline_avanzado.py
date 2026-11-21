"""Complemento académico del proyecto de Ingeniería de Datos para Spotify.

Incluye extracción por API pública (iTunes Search), pipeline ETL por capas
Bronze/Silver/Gold, estadísticas con NumPy/Pandas, modelos estrella/copo de nieve,
procesamiento batch opcional con Spark y demo de ventanas/watermark/checkpoint.

Nota: el API de iTunes aporta metadatos de catálogo; no proporciona reproducciones
reales de Spotify. Los indicadores de streams deben provenir de los datasets base.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent
DATA = BASE / "data_lake"
BRONZE = DATA / "bronze"
SILVER = DATA / "silver"
GOLD = DATA / "gold"


def _guardar_tabla(df: pd.DataFrame, ruta_sin_extension: Path) -> Path:
    """Guarda Parquet si hay motor instalado; de lo contrario usa CSV."""
    ruta_sin_extension.parent.mkdir(parents=True, exist_ok=True)
    try:
        destino = ruta_sin_extension.with_suffix(".parquet")
        df.to_parquet(destino, index=False)
    except (ImportError, ModuleNotFoundError, ValueError):
        destino = ruta_sin_extension.with_suffix(".csv")
        df.to_csv(destino, index=False, encoding="utf-8-sig")
    return destino


def _leer_tabla(carpeta: Path) -> pd.DataFrame:
    archivos = sorted(list(carpeta.glob("*.parquet")) + list(carpeta.glob("*.csv")))
    if not archivos:
        raise FileNotFoundError(f"No hay datos en {carpeta}. Ejecuta primero la extracción o el ETL.")
    partes = [pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p) for p in archivos]
    return pd.concat(partes, ignore_index=True)


def extraer_api_itunes(termino: str = "Coldplay", limite: int = 50) -> pd.DataFrame:
    """Extrae metadatos musicales de la API pública de iTunes Search."""
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError("Instala requests: pip install requests") from exc
    url = "https://itunes.apple.com/search"
    respuesta = requests.get(url, params={"term": termino, "entity": "song", "limit": limite}, timeout=30)
    respuesta.raise_for_status()
    resultados = respuesta.json().get("results", [])
    df = pd.DataFrame([{
        "track_id_api": r.get("trackId"),
        "titulo_track": r.get("trackName"),
        "nombre_artista": r.get("artistName"),
        "album": r.get("collectionName"),
        "genero_musical": r.get("primaryGenreName"),
        "duracion_ms": r.get("trackTimeMillis"),
        "fecha_lanzamiento": r.get("releaseDate"),
        "url_preview": r.get("previewUrl"),
        "fuente": "iTunes Search API",
        "fecha_extraccion_utc": datetime.now(timezone.utc).isoformat(),
    } for r in resultados])
    BRONZE.mkdir(parents=True, exist_ok=True)
    destino = _guardar_tabla(df, BRONZE / f"itunes_{datetime.now():%Y%m%d_%H%M%S}")
    print(f"API: {len(df)} registros extraídos. Capa Bronze: {destino}")
    print("Nota: son metadatos de catálogo, no cifras reales de streams de Spotify.")
    return df


def ejecutar_etl(df_origen: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """ETL didáctico: Bronze (crudo), Silver (limpio) y Gold (agregado)."""
    if df_origen is None:
        df_origen = _leer_tabla(BRONZE)
    BRONZE.mkdir(parents=True, exist_ok=True)
    SILVER.mkdir(parents=True, exist_ok=True)
    GOLD.mkdir(parents=True, exist_ok=True)
    _guardar_tabla(df_origen, BRONZE / "catalogo_origen")

    silver = df_origen.copy()
    for col in silver.select_dtypes(include=["object", "string"]).columns:
        silver[col] = silver[col].astype("string").str.strip().replace("", pd.NA)
    if "titulo_track" in silver:
        silver = silver.dropna(subset=["titulo_track"])
    claves = [c for c in ["titulo_track", "nombre_artista"] if c in silver.columns]
    if claves:
        silver = silver.drop_duplicates(subset=claves, keep="first")
    if "duracion_ms" in silver:
        silver["duracion_ms"] = pd.to_numeric(silver["duracion_ms"], errors="coerce")
    if "fecha_lanzamiento" in silver:
        silver["fecha_lanzamiento"] = pd.to_datetime(silver["fecha_lanzamiento"], errors="coerce", utc=True)
    silver = silver.reset_index(drop=True)
    silver_path = _guardar_tabla(silver, SILVER / "catalogo_limpio")

    if {"nombre_artista", "genero_musical"}.issubset(silver.columns):
        gold = (silver.groupby(["nombre_artista", "genero_musical"], dropna=False)
                .size().reset_index(name="cantidad_tracks"))
    elif "nombre_artista" in silver:
        gold = silver.groupby("nombre_artista", dropna=False).size().reset_index(name="cantidad_tracks")
    else:
        gold = pd.DataFrame({"registros": [len(silver)]})
    gold_path = _guardar_tabla(gold, GOLD / "tracks_por_artista_genero")
    metadata = {"pipeline": "Spotify Data Lakehouse", "ejecutado_utc": datetime.now(timezone.utc).isoformat(),
                "filas_bronze": int(len(df_origen)), "filas_silver": int(len(silver)),
                "filas_gold": int(len(gold)), "silver_path": str(silver_path), "gold_path": str(gold_path)}
    (DATA / "metadata_ultima_ejecucion.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    print("ETL completado:", json.dumps(metadata, indent=2, ensure_ascii=False))
    return silver, gold


def resumen_numpy_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Ejemplo reproducible de análisis vectorizado con NumPy y Pandas."""
    if "duracion_ms" not in df.columns:
        raise ValueError("La tabla no contiene duracion_ms. Extrae primero datos desde iTunes.")
    duracion = pd.to_numeric(df["duracion_ms"], errors="coerce").dropna().to_numpy(dtype=float)
    if duracion.size == 0:
        raise ValueError("No hay duraciones numéricas para analizar.")
    minutos = np.round(duracion / 60000, 2)
    resultado = pd.DataFrame({"duracion_min": minutos})
    print("Resumen vectorizado (NumPy + Pandas)")
    print(f"Cantidad: {duracion.size}; media: {np.mean(minutos):.2f} min; mediana: {np.median(minutos):.2f} min")
    print(f"Desviación estándar: {np.std(minutos):.2f} min; P90: {np.percentile(minutos, 90):.2f} min")
    print("Tracks de más de 5 minutos:", int(np.sum(minutos > 5)))
    return resultado


def construir_modelos_dimensionales(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Construye dimensiones y hechos de ejemplo en estrella y copo de nieve."""
    requeridas = {"titulo_track", "nombre_artista"}
    if not requeridas.issubset(df.columns):
        raise ValueError(f"Se requieren las columnas: {sorted(requeridas)}")
    tracks = df.copy().dropna(subset=["titulo_track", "nombre_artista"])
    dim_artista = tracks[["nombre_artista"]].drop_duplicates().reset_index(drop=True)
    dim_artista.insert(0, "artista_key", range(1, len(dim_artista) + 1))
    dim_genero = pd.DataFrame()
    if "genero_musical" in tracks:
        dim_genero = tracks[["genero_musical"]].drop_duplicates().reset_index(drop=True)
        dim_genero.insert(0, "genero_key", range(1, len(dim_genero) + 1))
    dim_track = tracks[[c for c in ["titulo_track", "nombre_artista", "album", "duracion_ms", "genero_musical"] if c in tracks]].drop_duplicates().reset_index(drop=True)
    dim_track.insert(0, "track_key", range(1, len(dim_track) + 1))
    dim_track = dim_track.merge(dim_artista, on="nombre_artista", how="left") if "nombre_artista" in dim_track else dim_track
    # La dimensión de track conserva artista en el modelo estrella.
    hecho = dim_track[["track_key", "artista_key"]].copy()
    hecho.insert(0, "reproduccion_key", range(1, len(hecho) + 1))
    hecho["cantidad_registros_catalogo"] = 1
    if not dim_genero.empty and "genero_musical" in dim_track:
        hecho = hecho.merge(dim_track[["track_key", "genero_musical"]].merge(dim_genero, on="genero_musical"), on="track_key", how="left")
    # Copo de nieve: la dimensión Track referencia por separado a Artista y Género.
    dim_track_snow = dim_track.drop(columns=[c for c in ["nombre_artista", "genero_musical"] if c in dim_track])
    if not dim_genero.empty:
        dim_track_snow = dim_track_snow.merge(dim_track[["track_key", "genero_musical"]].merge(dim_genero, on="genero_musical"), on="track_key", how="left")
    tablas = {"dim_artista": dim_artista, "dim_genero": dim_genero, "dim_track_estrella": dim_track,
              "hecho_catalogo": hecho, "dim_track_copo_nieve": dim_track_snow}
    for nombre, tabla in tablas.items():
        _guardar_tabla(tabla, GOLD / nombre)
    print("Modelos dimensionales guardados en data_lake/gold:", ", ".join(tablas))
    return tablas


def ejecutar_spark_batch():
    """Procesa los datos Silver con Spark batch (opcional; requiere Java y PySpark)."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import count, col
    except ImportError as exc:
        raise RuntimeError("Instala pyspark y Java para ejecutar Spark batch.") from exc
    archivos = list(SILVER.glob("*.parquet")) + list(SILVER.glob("*.csv"))
    if not archivos:
        raise FileNotFoundError("No hay datos Silver. Ejecuta primero el ETL.")
    spark = SparkSession.builder.appName("SpotifyBatchETL").master("local[*]").getOrCreate()
    try:
        fuente = str(archivos[0])
        df = spark.read.parquet(fuente) if fuente.endswith(".parquet") else spark.read.option("header", True).csv(fuente, inferSchema=True)
        if "nombre_artista" not in df.columns:
            raise ValueError("Silver no contiene nombre_artista.")
        resultado = df.groupBy("nombre_artista").agg(count("*").alias("cantidad_tracks")).orderBy(col("cantidad_tracks").desc())
        resultado.show(20, truncate=False)
        salida = GOLD / "spark_batch_artistas"
        resultado.write.mode("overwrite").parquet(str(salida))
        print("Resultado Spark batch guardado en:", salida)
    finally:
        spark.stop()


def crear_eventos_demo() -> Path:
    """Crea archivos JSON de eventos con event time y processing time."""
    entrada = DATA / "stream_input"
    entrada.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now(timezone.utc)
    eventos = []
    for i in range(18):
        eventos.append({"evento_id": f"STR-{i+1:03d}", "artista": ["Adele", "Coldplay", "Shakira"][i % 3],
                        "reproducciones": 10 + i * 5,
                        "event_time": (ahora - pd.Timedelta(minutes=(i % 7))).isoformat()})
    # Evento deliberadamente antiguo para ilustrar el tratamiento de datos tardíos.
    eventos.append({"evento_id": "STR-LATE", "artista": "Adele", "reproducciones": 99,
                    "event_time": (ahora - pd.Timedelta(hours=2)).isoformat()})
    ruta = entrada / f"eventos_{int(time.time())}.json"
    with ruta.open("w", encoding="utf-8") as f:
        for evento in eventos:
            f.write(json.dumps(evento, ensure_ascii=False) + "\n")
    print("Eventos de demostración escritos en:", ruta)
    return entrada


def spark_streaming_ventanas():
    """Ventanas por tiempo de evento, watermark, checkpoint y salida Parquet."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, window, sum as sum_, current_timestamp
        from pyspark.sql.types import StructType, StructField, StringType, LongType
    except ImportError as exc:
        raise RuntimeError("Instala pyspark y Java para ejecutar Structured Streaming.") from exc
    entrada = crear_eventos_demo()
    salida = DATA / "stream_output" / "ventanas_artista"
    checkpoint = DATA / "checkpoints" / "ventanas_artista"
    schema = StructType([StructField("evento_id", StringType()), StructField("artista", StringType()),
                         StructField("reproducciones", LongType()), StructField("event_time", StringType())])
    spark = (SparkSession.builder.appName("SpotifyStreamingVentanas").master("local[2]")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    try:
        origen = spark.readStream.schema(schema).json(str(entrada))
        preparado = (origen.withColumn("event_time", col("event_time").cast("timestamp"))
                     .withColumn("processing_time", current_timestamp()))
        agregado = (preparado.withWatermark("event_time", "10 minutes")
                    .groupBy(window(col("event_time"), "5 minutes", "5 minutes"), col("artista"))
                    .agg(sum_("reproducciones").alias("reproducciones_ventana")))
        consulta = (agregado.writeStream.format("parquet").outputMode("append")
                    .option("path", str(salida)).option("checkpointLocation", str(checkpoint))
                    .trigger(once=True).start())
        consulta.awaitTermination()
        print("Ventanas procesadas. Salida:", salida)
        print("Checkpoint:", checkpoint)
        print("Event time = momento declarado del evento; processing time = momento de procesamiento.")
        print("Watermark de 10 min = umbral para gestionar eventos tardíos y limpiar estado; no recupera por sí solo eventos descartados.")
        print("El checkpoint permite a Spark recuperar progreso/estado compatible al reiniciar la consulta.")
    finally:
        spark.stop()


def menu():
    while True:
        print("""\n=== COMPLEMENTO AVANZADO — INGENIERÍA DE DATOS SPOTIFY ===
1. Extraer metadatos desde API pública iTunes (Bronze)
2. Ejecutar pipeline ETL Bronze -> Silver -> Gold
3. Estadísticas con NumPy y Pandas (sobre datos Silver)
4. Construir modelos estrella y copo de nieve
5. Procesamiento batch con Apache Spark
6. Streaming con ventanas, Event Time, Watermark y checkpoint
0. Salir""")
        op = input("Opción: ").strip()
        try:
            if op == "1":
                termino = input("Artista o término de búsqueda [Coldplay]: ").strip() or "Coldplay"
                limite_txt = input("Cantidad máxima [50]: ").strip()
                extraer_api_itunes(termino, int(limite_txt or 50))
            elif op == "2":
                ejecutar_etl()
            elif op == "3":
                resumen_numpy_pandas(_leer_tabla(SILVER))
            elif op == "4":
                construir_modelos_dimensionales(_leer_tabla(SILVER))
            elif op == "5":
                ejecutar_spark_batch()
            elif op == "6":
                spark_streaming_ventanas()
            elif op == "0":
                break
            else:
                print("Opción no válida.")
        except Exception as exc:
            print(f"No se pudo completar la operación: {exc}")


if __name__ == "__main__":
    menu()
