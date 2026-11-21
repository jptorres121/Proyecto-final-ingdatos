import gc
import json
import os
import sys

import pandas as pd

try:
    import kagglehub
except ImportError:
    print("Falta la librería 'kagglehub'.")
    print("Instálala con: pip install kagglehub[pandas-datasets]")
    sys.exit(1)

try:
    import psutil
except ImportError:  # opcional: sin psutil se usa 'resource' (Linux/macOS) como alternativa
    psutil = None


# Configuración de pandas para que se vea mejor en consola
pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 160)


# =============================================================================
# DATASETS DISPONIBLES
# =============================================================================

DATASETS = {
    "1": {
        "nombre": "Spotify Dataset 1921-2020 (600k Tracks)",
        "handle": "yamaerenay/spotify-dataset-19212020-600k-tracks",
    },
    "2": {
        "nombre": "Spotify200 (Top 200 semanal)",
        "handle": "yelexa/spotify200",
    },
}

# Caché en memoria para no volver a descargar / releer en cada operación
_cache_rutas = {}
_cache_dataframes = {}


# =============================================================================
# CARGA DE DATOS
# =============================================================================

def descargar_dataset(handle):
    """Descarga (o reutiliza la copia local en caché de kagglehub) el dataset completo."""
    if handle in _cache_rutas:
        return _cache_rutas[handle]

    print(f"\nDescargando / verificando dataset '{handle}' desde Kaggle...")
    ruta = kagglehub.dataset_download(handle)
    _cache_rutas[handle] = ruta
    print(f"Dataset disponible en: {ruta}")
    return ruta


def listar_archivos(ruta):
    """Lista los archivos de datos (csv/json/parquet) dentro del dataset descargado."""
    archivos = []
    for root, _, files in os.walk(ruta):
        for f in files:
            if f.lower().endswith((".csv", ".json", ".parquet")):
                archivos.append(os.path.join(root, f))
    return sorted(archivos)


def elegir_archivo(archivos):
    print("\nArchivos disponibles en el dataset:")
    print("(Para el esquema estrella, las canciones están en tracks.csv)")
    for i, a in enumerate(archivos, start=1):
        print(f"  {i}. {os.path.relpath(a)}")

    while True:
        sel = input("Selecciona el número del archivo a cargar (o 'c' para cancelar): ").strip()
        if sel.lower() == "c":
            return None
        if sel.isdigit() and 1 <= int(sel) <= len(archivos):
            return archivos[int(sel) - 1]
        print("Opción inválida, intenta de nuevo.")


def leer_json(ruta_archivo):
    """Lee un JSON como tabla. Si no tiene forma tabular (p. ej. un diccionario cuyas listas
    tienen distinto largo, como dict_artists.json) lo convierte en una tabla clave / valor."""
    try:
        return pd.read_json(ruta_archivo)
    except ValueError:
        with open(ruta_archivo, encoding="utf-8") as f:
            datos = json.load(f)
        if isinstance(datos, dict):
            print("El JSON no es tabular (es un diccionario); se carga como tabla clave / valor.")
            return pd.DataFrame(list(datos.items()), columns=["clave", "valor"])
        if isinstance(datos, list):
            return pd.json_normalize(datos)
        raise


def leer_archivo(ruta_archivo):
    print(f"\nCargando '{ruta_archivo}' con pandas...")
    if ruta_archivo.endswith(".csv"):
        return pd.read_csv(ruta_archivo, low_memory=False)
    if ruta_archivo.endswith(".json"):
        return leer_json(ruta_archivo)
    if ruta_archivo.endswith(".parquet"):
        return pd.read_parquet(ruta_archivo)
    raise ValueError("Formato de archivo no soportado.")


def elegir_archivo_auto(clave_dataset, archivos):
    """Elige el archivo principal sin pedir nada por consola (para dashboard/Streamlit)."""
    preferidos = {"1": ["tracks.csv"], "2": ["charts.csv", "spotify200.csv"]}
    por_nombre = {os.path.basename(a).lower(): a for a in archivos}
    for nombre in preferidos.get(clave_dataset, []):
        if nombre in por_nombre:
            return por_nombre[nombre]
    csvs = [a for a in archivos if a.lower().endswith(".csv")] or archivos
    return max(csvs, key=os.path.getsize)  # el archivo más grande suele ser la tabla principal


def cargar_dataframe(clave_dataset, interactivo=True):
    """Carga (con caché) el DataFrame de un dataset de Kaggle.
    interactivo=False elige el archivo automáticamente (nunca llama a input())."""
    info = DATASETS[clave_dataset]

    if clave_dataset in _cache_dataframes:
        return _cache_dataframes[clave_dataset], info["nombre"]

    ruta = descargar_dataset(info["handle"])
    archivos = listar_archivos(ruta)

    if not archivos:
        print("No se encontraron archivos csv/json/parquet en el dataset.")
        return None, info["nombre"]

    while True:
        if len(archivos) == 1:
            archivo = archivos[0]
        elif interactivo:
            archivo = elegir_archivo(archivos)
        else:
            archivo = elegir_archivo_auto(clave_dataset, archivos)
        if archivo is None:
            return None, info["nombre"]

        try:
            df = leer_archivo(archivo)
            break
        except Exception as e:
            print(f"Error al leer el archivo: {e}")
            if len(archivos) == 1 or not interactivo:
                return None, info["nombre"]
            print("Elige otro archivo de la lista.")

    _cache_dataframes[clave_dataset] = df
    _cache_rutas[f"archivo_{clave_dataset}"] = archivo
    return df, info["nombre"]


# =============================================================================
# EXPLORACIÓN GENERAL DE LOS DATOS (por dataset individual)
# =============================================================================

def mostrar_info_general(df, nombre):
    print(f"\n===== {nombre} =====")
    print(f"Filas: {df.shape[0]:,} | Columnas: {df.shape[1]}")
    print("\nColumnas y tipos de datos:")
    print(df.dtypes)
    print("\nPrimeros 5 registros:")
    print(df.head())


def mostrar_estadisticas(df):
    print("\nEstadísticas descriptivas (columnas numéricas):")
    print(df.describe())


def mostrar_nulos(df):
    print("\nValores nulos por columna:")
    print(df.isnull().sum())

# =============================================================================
# USO DE MEMORIA
# =============================================================================

def _fmt_bytes(n):
    """Convierte bytes a un texto legible (KB, MB, GB)."""
    n = float(n)
    for unidad in ("B", "KB", "MB", "GB"):
        if n < 1024 or unidad == "GB":
            return f"{n:,.2f} {unidad}" if unidad != "B" else f"{int(n)} B"
        n /= 1024


def memoria_proceso():
    """Devuelve (memoria_actual_bytes, descripcion) del proceso de Python, o None si no se puede medir."""
    if psutil is not None:
        return psutil.Process(os.getpid()).memory_info().rss, "RAM actual del proceso (RSS)"
    try:
        import resource
        maximo = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform != "darwin":  # en Linux viene en KB; en macOS ya viene en bytes
            maximo *= 1024
        return maximo, "RAM máxima alcanzada por el proceso"
    except ImportError:
        return None


def _mostrar_memoria_proceso():
    medicion = memoria_proceso()
    if medicion is None:
        print("Memoria del proceso: no disponible (instala 'psutil' con: pip install psutil)")
    else:
        print(f"{medicion[1]}: {_fmt_bytes(medicion[0])}")


def mostrar_memoria_dataframe(df, nombre):
    """Uso de memoria de un DataFrame: total y detalle por columna."""
    por_columna = df.memory_usage(deep=True, index=True)
    total = int(por_columna.sum())

    detalle = pd.DataFrame({
        "tipo": df.dtypes.astype(str),
        "memoria": por_columna.drop("Index", errors="ignore"),
    })
    detalle["% del total"] = (detalle["memoria"] / total * 100).round(1)
    detalle = detalle.sort_values("memoria", ascending=False)
    detalle["memoria"] = detalle["memoria"].apply(_fmt_bytes)

    print(f"\n===== Uso de memoria - {nombre} =====")
    print(f"Filas: {len(df):,} | Columnas: {df.shape[1]}")
    print(f"Memoria total del DataFrame: {_fmt_bytes(total)}")
    print("\nDetalle por columna (de mayor a menor):")
    print(detalle)
    print()
    _mostrar_memoria_proceso()


def mostrar_memoria_tablas(tablas, titulo):
    """Uso de memoria de las tablas del esquema estrella ya construidas.
    tablas: dict {nombre_tabla: DataFrame o None}."""
    construidas = {n: t for n, t in tablas.items() if t is not None}
    print(f"\n===== Memoria de las tablas generadas - {titulo} =====")
    if not construidas:
        print("Aún no has construido ninguna tabla (usa las opciones anteriores del menú).")
        return
    total = 0
    for nombre, tabla in construidas.items():
        uso = int(tabla.memory_usage(deep=True, index=True).sum())
        total += uso
        print(f"  {nombre:<18} {len(tabla):>12,} filas  {_fmt_bytes(uso):>12}")
    print(f"  {'TOTAL':<18} {'':>12}        {_fmt_bytes(total):>12}")
    print()
    _mostrar_memoria_proceso()


def mostrar_memoria_proyecto():
    """Resumen de la memoria que maneja el proyecto: datasets en caché y proceso."""
    print("\n===== Uso de memoria del proyecto =====")
    total = 0
    if not _cache_dataframes:
        print("No hay datasets cargados en memoria todavía.")
    for clave, df in _cache_dataframes.items():
        uso = int(df.memory_usage(deep=True, index=True).sum())
        total += uso
        print(f"  {DATASETS[clave]['nombre']}")
        print(f"      {len(df):,} filas x {df.shape[1]} columnas -> {_fmt_bytes(uso)}")
    print(f"\nTotal en DataFrames cacheados: {_fmt_bytes(total)}")
    _mostrar_memoria_proceso()


def liberar_memoria():
    """Vacía las cachés de datasets y fuerza la recolección de basura."""
    antes = memoria_proceso()
    _cache_dataframes.clear()
    _cache_rutas.clear()
    gc.collect()
    despues = memoria_proceso()
    print("\nCaché de datasets vaciada.")
    if antes and despues and psutil is not None:
        print(f"RAM del proceso: {_fmt_bytes(antes[0])} -> {_fmt_bytes(despues[0])}")
    print("(Los datasets se volverán a leer la próxima vez que los abras.)")


# =============================================================================
# UTILIDADES PARA COMBINAR / DEDUPLICAR DATOS DE VARIAS FUENTES
# =============================================================================

def buscar_columna(df, candidatos):
    """Busca la primera columna existente (sin importar mayúsculas/minúsculas)
    dentro de una lista de nombres candidatos."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidatos:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def limpiar_artista(valor):
    """Limpia valores de artista que a veces vienen como texto de lista,
    p. ej. "['Nombre del Artista']" -> "Nombre del Artista"."""
    if pd.isna(valor):
        return valor
    texto = str(valor).strip()
    if texto.startswith("[") and texto.endswith("]"):
        texto = texto.strip("[]")
        texto = texto.split(",")[0]  # si hay varios artistas, toma el primero
        texto = texto.strip().strip("'").strip('"')
    return texto


def normalizar(serie):
    """Normaliza texto (minúsculas, sin espacios extra) para poder comparar /
    deduplicar entre datasets distintos sin importar mayúsculas o espacios."""
    return serie.astype(str).str.strip().str.lower()


def _mostrar_tabla(df_tabla, nombre_tabla):
    if df_tabla is None:
        return
    print(f"\n{nombre_tabla} ({len(df_tabla):,} filas):")
    print(df_tabla.head(10))


# =============================================================================
# DUPLICADOS Y LIMPIEZA DE DATOS
# =============================================================================

COLUMNAS_TITULO = ["name", "track_name", "title", "titulo_track"]
COLUMNAS_ARTISTA = ["artists", "artist", "artist_name", "nombre_artista"]


def _columnas_texto(df):
    return df.select_dtypes(include=["object", "string"]).columns


def _contar_filas_duplicadas(df):
    """Cuenta las filas que son copia exacta de otra anterior (la primera aparición no cuenta)."""
    try:
        return int(df.duplicated(keep="first").sum())
    except TypeError:  # columnas con valores no hashables (listas, dicts de un JSON)
        return int(df.astype(str).duplicated(keep="first").sum())


def _artista_limpio(serie):
    """Aplica limpiar_artista solo a los valores únicos (mucho más rápido que fila por fila)."""
    mapa = {v: limpiar_artista(v) for v in serie.dropna().unique()}
    return serie.map(mapa)


def _clave_cancion(df):
    """Serie 'título||artista' normalizada para identificar canciones; None si no hay columna de título.
    Las filas sin título quedan como NA."""
    col_titulo = buscar_columna(df, COLUMNAS_TITULO)
    if not col_titulo:
        return None
    titulo = normalizar(df[col_titulo].fillna(""))
    col_artista = buscar_columna(df, COLUMNAS_ARTISTA)
    artista = normalizar(_artista_limpio(df[col_artista]).fillna("")) if col_artista else ""
    clave = titulo + "||" + artista
    return clave.where(titulo != "", pd.NA)


def mostrar_duplicados(df, nombre):
    """Informe de duplicados de un dataset: filas idénticas y canciones repetidas."""
    total = len(df)
    exactas = _contar_filas_duplicadas(df)

    print(f"\n===== Duplicados - {nombre} =====")
    print(f"Filas totales: {total:,}")
    print(f"Filas duplicadas exactas (todas las columnas iguales): {exactas:,} "
          f"({exactas / total * 100 if total else 0:.2f} %)")

    clave = _clave_cancion(df)
    if clave is None:
        print("No hay columna de título; no se puede contar canciones repetidas.")
    else:
        con_clave = clave.dropna()
        unicas = con_clave.nunique()
        repetidas = len(con_clave) - unicas
        print(f"Canciones únicas (título + artista): {unicas:,}")
        print(f"Registros que repiten una canción ya vista: {repetidas:,}")
        print("  (en datasets de rankings semanales esto es normal: una canción aparece en varias semanas)")

    return {"filas": total, "duplicadas_exactas": exactas}


def mostrar_duplicados_combinados(df1, nombre1, df2, nombre2):
    """Duplicados de cada dataset y canciones que se repiten entre ambos."""
    for df, nombre in ((df1, nombre1), (df2, nombre2)):
        if df is not None:
            mostrar_duplicados(df, nombre)

    if df1 is None or df2 is None:
        return
    c1, c2 = _clave_cancion(df1), _clave_cancion(df2)
    if c1 is None or c2 is None:
        print("\nUno de los datasets no tiene columna de título; no se puede comparar entre ambos.")
        return

    s1, s2 = set(c1.dropna().unique()), set(c2.dropna().unique())
    comunes = s1 & s2
    print("\n===== Duplicados ENTRE ambos datasets =====")
    print(f"Canciones únicas en '{nombre1}': {len(s1):,}")
    print(f"Canciones únicas en '{nombre2}': {len(s2):,}")
    print(f"Canciones presentes en los dos: {len(comunes):,}")
    print(f"Canciones únicas tras la fusión: {len(s1 | s2):,}")
    print(f"Registros de canción que la fusión elimina por estar repetidos entre datasets: {len(comunes):,}")


def limpiar_dataframe(df, nombre):
    """Limpia un dataset y devuelve el DataFrame limpio. Pasos:
    1. Quita espacios sobrantes en textos y convierte textos vacíos en nulos.
    2. Elimina filas completamente vacías.
    3. Elimina filas sin título de canción (si el dataset tiene esa columna).
    4. Elimina filas duplicadas exactas.
    """
    print(f"\n===== Limpieza - {nombre} =====")
    filas_inicio = len(df)
    nulos_inicio = int(df.isnull().sum().sum())
    memoria_inicio = int(df.memory_usage(deep=True, index=True).sum())
    df = df.copy()
    reporte = []

    # 1. Espacios y textos vacíos
    modificados = 0
    for col in _columnas_texto(df):
        original = df[col]
        sin_espacios = original.str.strip()
        # los valores que no son texto dentro de una columna 'object' se dejan como estaban
        limpio = sin_espacios.where(sin_espacios.notna() | original.isna(), original)
        limpio = limpio.mask(limpio == "", pd.NA)
        modificados += int((limpio.fillna("<NA>") != original.fillna("<NA>")).sum())
        df[col] = limpio
    reporte.append(("Valores de texto corregidos (espacios / vacíos -> nulo)", modificados))

    # 2. Filas totalmente vacías
    antes = len(df)
    df = df.dropna(how="all")
    reporte.append(("Filas completamente vacías eliminadas", antes - len(df)))

    # 3. Filas sin título
    col_titulo = buscar_columna(df, COLUMNAS_TITULO)
    if col_titulo:
        antes = len(df)
        df = df.dropna(subset=[col_titulo])
        reporte.append((f"Filas sin '{col_titulo}' eliminadas", antes - len(df)))

    # 4. Duplicados exactos
    antes = len(df)
    try:
        df = df.drop_duplicates()
    except TypeError:
        df = df[~df.astype(str).duplicated(keep="first")]
    reporte.append(("Filas duplicadas exactas eliminadas", antes - len(df)))

    df = df.reset_index(drop=True)

    for texto, cantidad in reporte:
        print(f"  {texto}: {cantidad:,}")
    memoria_fin = int(df.memory_usage(deep=True, index=True).sum())
    print(f"\nFilas: {filas_inicio:,} -> {len(df):,}  (se quitaron {filas_inicio - len(df):,})")
    print(f"Valores nulos totales: {nulos_inicio:,} -> {int(df.isnull().sum().sum()):,}")
    print(f"Memoria del DataFrame: {_fmt_bytes(memoria_inicio)} -> {_fmt_bytes(memoria_fin)}")
    return df


# =============================================================================
# ESQUEMA ESTRELLA POR DATASET INDIVIDUAL
# =============================================================================

def construir_dim_tiempo(df):
    col_fecha = buscar_columna(df, ["date", "release_date", "week", "chart_week", "fecha"])
    if not col_fecha:
        print("No se encontró columna de fecha en este dataset; se omite dim_tiempo.")
        return None

    fechas = pd.to_datetime(df[col_fecha], errors="coerce").dropna().dt.date
    fechas_unicas = pd.Series(fechas.unique(), name="fecha").sort_values().reset_index(drop=True)

    dim = pd.DataFrame({"fecha": fechas_unicas})
    fecha_dt = pd.to_datetime(dim["fecha"])

    dim.insert(0, "tiempo_key", range(1, len(dim) + 1))
    dim["anio"] = fecha_dt.dt.year
    dim["mes"] = fecha_dt.dt.month
    dim["nombre_mes"] = fecha_dt.dt.month_name()
    dim["dia_mes"] = fecha_dt.dt.day
    dim["dia_semana"] = fecha_dt.dt.dayofweek
    dim["nombre_dia_semana"] = fecha_dt.dt.day_name()
    dim["trimestre"] = fecha_dt.dt.quarter
    dim["es_fin_de_semana"] = dim["dia_semana"].isin([5, 6])
    # 'hora' no existe en el dataset de origen (solo hay fecha) -> se omite
    return dim


def construir_dim_artista(df):
    col_artista = buscar_columna(df, ["artists", "artist", "artist_name", "nombre_artista"])
    if not col_artista:
        print("No se encontró columna de artista en este dataset; se omite dim_artista.")
        return None

    artistas = df[[col_artista]].dropna().copy()
    artistas.columns = ["nombre_artista"]
    artistas["nombre_artista"] = artistas["nombre_artista"].apply(limpiar_artista)
    artistas = artistas.drop_duplicates().reset_index(drop=True)
    artistas.insert(0, "artista_key", range(1, len(artistas) + 1))
    artistas["artista_id"] = artistas["artista_key"].apply(lambda x: f"ART-{x}")
    # 'sello_discografico' y 'pais_artista' no existen en el dataset de origen -> se omiten
    return artistas


def construir_dim_track(df):
    col_titulo = buscar_columna(df, ["name", "track_name", "title", "titulo_track"])
    if not col_titulo:
        print("No se encontró columna de título de canción; se omite dim_track.")
        return None

    col_dur = buscar_columna(df, ["duration_ms", "duracion_total_ms"])
    col_genero = buscar_columna(df, ["genre", "track_genre", "genero_musical"])
    col_pop = buscar_columna(df, ["popularity", "popularidad_actual"])

    seleccion = [col_titulo]
    rename = {col_titulo: "titulo_track"}
    for col, nuevo_nombre in ((col_dur, "duracion_total_ms"),
                               (col_genero, "genero_musical"),
                               (col_pop, "popularidad_actual")):
        if col:
            seleccion.append(col)
            rename[col] = nuevo_nombre

    tracks = df[seleccion].dropna(subset=[col_titulo]).drop_duplicates().reset_index(drop=True)
    tracks = tracks.rename(columns=rename)
    tracks.insert(0, "track_key", range(1, len(tracks) + 1))
    tracks["track_id"] = tracks["track_key"].apply(lambda x: f"TRK-{x}")
    # 'idioma' no existe en el dataset de origen -> se omite
    return tracks


def construir_hecho_streaming(df, dim_track, dim_tiempo):
    if dim_track is None:
        print("No hay dim_track disponible; se omite hecho_streaming.")
        return None

    col_titulo = buscar_columna(df, ["name", "track_name", "title"])
    col_fecha = buscar_columna(df, ["date", "release_date", "week", "chart_week"])
    col_streams = buscar_columna(df, ["streams", "playcount", "cantidad_reproducciones"])

    if not col_titulo:
        print("No se encontró columna de título para relacionar con dim_track; se omite hecho_streaming.")
        return None

    hecho = pd.DataFrame({"titulo_track": df[col_titulo]})
    hecho = hecho.merge(dim_track[["track_key", "titulo_track"]], on="titulo_track", how="left")

    if col_fecha and dim_tiempo is not None:
        hecho["fecha"] = pd.to_datetime(df[col_fecha], errors="coerce").dt.date
        hecho = hecho.merge(dim_tiempo[["tiempo_key", "fecha"]], on="fecha", how="left")
        hecho = hecho.drop(columns=["fecha"])

    hecho["cantidad_reproducciones"] = df[col_streams] if col_streams else 1

    hecho = hecho.drop(columns=["titulo_track"]).dropna(subset=["track_key"])
    hecho.insert(0, "streaming_id", range(1, len(hecho) + 1))
    # 'tiempo_reproducido_ms', 'reproduccion_completa_flag', 'usuario_key',
    # 'dispositivo_key' y 'contexto_key' no existen en el dataset de origen -> se omiten
    return hecho


# =============================================================================
# ESQUEMA ESTRELLA UNIFICADO (los dos datasets combinados en una sola tabla
# por dimensión / hecho, deduplicando registros repetidos entre ambos)
# =============================================================================

def construir_dim_artista_combinado(df1, df2):
    piezas = []
    for df in (df1, df2):
        if df is None:
            continue
        col = buscar_columna(df, ["artists", "artist", "artist_name", "nombre_artista"])
        if col:
            piezas.append(df[col].dropna().apply(limpiar_artista))

    if not piezas:
        print("Ninguno de los datasets tiene columna de artista; se omite dim_artista.")
        return None

    todos = pd.concat(piezas, ignore_index=True)
    todos = todos[todos.astype(str).str.strip() != ""]

    dim = pd.DataFrame({"nombre_artista": todos})
    dim["_clave"] = normalizar(dim["nombre_artista"])
    dim = dim.drop_duplicates(subset="_clave").drop(columns="_clave").reset_index(drop=True)
    dim.insert(0, "artista_key", range(1, len(dim) + 1))
    dim["artista_id"] = dim["artista_key"].apply(lambda x: f"ART-{x}")
    # 'sello_discografico' y 'pais_artista' no existen en ninguno de los dos datasets -> se omiten
    return dim


def construir_dim_track_combinado(df1, df2, dim_artista):
    piezas = []
    for df in (df1, df2):
        if df is None:
            continue
        col_titulo = buscar_columna(df, ["name", "track_name", "title", "titulo_track"])
        if not col_titulo:
            continue

        col_artista = buscar_columna(df, ["artists", "artist", "artist_name"])
        col_dur = buscar_columna(df, ["duration_ms", "duracion_total_ms"])
        col_genero = buscar_columna(df, ["genre", "track_genre", "genero_musical"])
        col_pop = buscar_columna(df, ["popularity", "popularidad_actual"])

        pieza = pd.DataFrame()
        pieza["titulo_track"] = df[col_titulo]
        pieza["nombre_artista"] = df[col_artista].apply(limpiar_artista) if col_artista else pd.NA
        pieza["duracion_total_ms"] = df[col_dur] if col_dur else pd.NA
        pieza["genero_musical"] = df[col_genero] if col_genero else pd.NA
        pieza["popularidad_actual"] = df[col_pop] if col_pop else pd.NA
        piezas.append(pieza)

    if not piezas:
        print("Ninguno de los datasets tiene columna de título de canción; se omite dim_track.")
        return None

    combinado = pd.concat(piezas, ignore_index=True).dropna(subset=["titulo_track"])
    combinado["_clave_track"] = normalizar(combinado["titulo_track"])
    combinado["_clave_artista"] = normalizar(combinado["nombre_artista"].fillna(""))

    # Se agrupa por título + artista (incluso si viene de datasets distintos) y se
    # toma el primer valor no nulo de cada columna para completar la información.
    agrupado = (
        combinado.groupby(["_clave_track", "_clave_artista"], as_index=False)
        .agg({
            "titulo_track": "first",
            "nombre_artista": "first",
            "duracion_total_ms": "first",
            "genero_musical": "first",
            "popularidad_actual": "first",
        })
    )

    if dim_artista is not None:
        dim_artista_tmp = dim_artista.copy()
        dim_artista_tmp["_clave_artista"] = normalizar(dim_artista_tmp["nombre_artista"])
        agrupado = agrupado.merge(
            dim_artista_tmp[["_clave_artista", "artista_key"]], on="_clave_artista", how="left"
        )
    else:
        agrupado["artista_key"] = pd.NA

    agrupado = agrupado.drop(columns=["_clave_track", "_clave_artista", "nombre_artista"])
    # Se quitan columnas que quedaron completamente vacías (ej. si ningún dataset trajo duración)
    agrupado = agrupado.dropna(axis=1, how="all")
    agrupado.insert(0, "track_key", range(1, len(agrupado) + 1))
    agrupado["track_id"] = agrupado["track_key"].apply(lambda x: f"TRK-{x}")
    # 'idioma' no existe en ninguno de los dos datasets -> se omite
    return agrupado


def construir_dim_tiempo_combinado(df1, df2):
    piezas = []
    for df in (df1, df2):
        if df is None:
            continue
        col_fecha = buscar_columna(df, ["date", "release_date", "week", "chart_week", "fecha"])
        if col_fecha:
            piezas.append(pd.to_datetime(df[col_fecha], errors="coerce").dropna().dt.date)

    if not piezas:
        print("Ninguno de los datasets tiene columna de fecha; se omite dim_tiempo.")
        return None

    fechas_unicas = pd.Series(pd.concat(piezas, ignore_index=True).unique(), name="fecha")
    fechas_unicas = fechas_unicas.sort_values().reset_index(drop=True)

    dim = pd.DataFrame({"fecha": fechas_unicas})
    fecha_dt = pd.to_datetime(dim["fecha"])
    dim.insert(0, "tiempo_key", range(1, len(dim) + 1))
    dim["anio"] = fecha_dt.dt.year
    dim["mes"] = fecha_dt.dt.month
    dim["nombre_mes"] = fecha_dt.dt.month_name()
    dim["dia_mes"] = fecha_dt.dt.day
    dim["dia_semana"] = fecha_dt.dt.dayofweek
    dim["nombre_dia_semana"] = fecha_dt.dt.day_name()
    dim["trimestre"] = fecha_dt.dt.quarter
    dim["es_fin_de_semana"] = dim["dia_semana"].isin([5, 6])
    # 'hora' no existe en ninguno de los dos datasets -> se omite
    return dim


def construir_hecho_streaming_combinado(fuentes, dim_track, dim_tiempo):
    """fuentes: lista de tuplas (nombre_dataset, dataframe)."""
    if dim_track is None:
        print("No hay dim_track disponible; se omite hecho_streaming.")
        return None

    partes = []
    for nombre_fuente, df in fuentes:
        if df is None:
            continue
        col_streams = buscar_columna(df, ["streams", "playcount", "cantidad_reproducciones"])
        col_titulo = buscar_columna(df, ["name", "track_name", "title"])
        col_fecha = buscar_columna(df, ["date", "release_date", "week", "chart_week"])

        if not col_streams or not col_titulo:
            print(f"'{nombre_fuente}' no tiene una métrica real de reproducciones; no aporta filas a hecho_streaming.")
            continue

        parte = pd.DataFrame()
        parte["_clave_track"] = normalizar(df[col_titulo])
        parte["cantidad_reproducciones"] = df[col_streams]
        parte["dataset_origen"] = nombre_fuente  # columna extra informativa (no está en el esquema original)
        if col_fecha:
            parte["fecha"] = pd.to_datetime(df[col_fecha], errors="coerce").dt.date
        partes.append(parte)

    if not partes:
        print("Ningún dataset tiene una métrica real de reproducciones; se omite hecho_streaming.")
        return None

    hechos = pd.concat(partes, ignore_index=True)

    dim_track_tmp = dim_track.copy()
    dim_track_tmp["_clave_track"] = normalizar(dim_track_tmp["titulo_track"])
    hechos = hechos.merge(dim_track_tmp[["_clave_track", "track_key"]], on="_clave_track", how="left")
    hechos = hechos.drop(columns=["_clave_track"]).dropna(subset=["track_key"])

    if "fecha" in hechos.columns and dim_tiempo is not None:
        hechos = hechos.merge(dim_tiempo[["tiempo_key", "fecha"]], on="fecha", how="left")
        hechos = hechos.drop(columns=["fecha"])

    hechos.insert(0, "streaming_id", range(1, len(hechos) + 1))
    # 'tiempo_reproducido_ms', 'reproduccion_completa_flag', 'usuario_key',
    # 'dispositivo_key' y 'contexto_key' no existen en ninguno de los datasets -> se omiten
    return hechos


# =============================================================================
# MENÚS
# =============================================================================

def menu_estructura(df, nombre):
    """Menú para ver la estructura tipo Data Warehouse de UN solo dataset."""
    contexto = {}

    while True:
        print(f"\n--- Esquema estrella (solo tablas disponibles) - {nombre} ---")
        print("1. Ver dim_tiempo")
        print("2. Ver dim_artista")
        print("3. Ver dim_track")
        print("4. Ver hecho_streaming")
        print("5. Ver uso de memoria de las tablas generadas")
        print("6. Volver")
        op = input("Selecciona una opción: ").strip()

        if op == "1":
            contexto.setdefault("tiempo", construir_dim_tiempo(df))
            _mostrar_tabla(contexto["tiempo"], "dim_tiempo")
        elif op == "2":
            contexto.setdefault("artista", construir_dim_artista(df))
            _mostrar_tabla(contexto["artista"], "dim_artista")
        elif op == "3":
            contexto.setdefault("track", construir_dim_track(df))
            _mostrar_tabla(contexto["track"], "dim_track")
        elif op == "4":
            contexto.setdefault("tiempo", construir_dim_tiempo(df))
            contexto.setdefault("track", construir_dim_track(df))
            contexto.setdefault(
                "hecho",
                construir_hecho_streaming(df, contexto.get("track"), contexto.get("tiempo")),
            )
            _mostrar_tabla(contexto["hecho"], "hecho_streaming")
        elif op == "5":
            mostrar_memoria_tablas(
                {"dim_tiempo": contexto.get("tiempo"), "dim_artista": contexto.get("artista"),
                 "dim_track": contexto.get("track"), "hecho_streaming": contexto.get("hecho")},
                nombre,
            )
        elif op == "6":
            break
        else:
            print("Opción inválida.")


def menu_dataset(clave):
    df, nombre = cargar_dataframe(clave)
    if df is None:
        return

    while True:
        print(f"\n=== {nombre} ===")
        print("1. Ver información general (shape, tipos, primeros registros)")
        print("2. Ver estadísticas descriptivas")
        print("3. Ver valores nulos")
        print("4. Ver estructura tipo Data Warehouse (dimensiones y hechos)")
        print("5. Ver duplicados")
        print("6. Limpiar datos (quita duplicados, vacíos y espacios)")
        print("7. Ver uso de memoria de este dataset")
        print("8. Volver al menú principal")
        op = input("Selecciona una opción: ").strip()

        if op == "1":
            mostrar_info_general(df, nombre)
        elif op == "2":
            mostrar_estadisticas(df)
        elif op == "3":
            mostrar_nulos(df)
        elif op == "4":
            menu_estructura(df, nombre)
        elif op == "5":
            mostrar_duplicados(df, nombre)
        elif op == "6":
            df = limpiar_dataframe(df, nombre)
            _cache_dataframes[clave] = df  # el resto del programa usa ya la versión limpia
        elif op == "7":
            mostrar_memoria_dataframe(df, nombre)
        elif op == "8":
            break
        else:
            print("Opción inválida.")


def menu_combinado():
    """Menú para ver AMBOS datasets juntos en un solo esquema estrella unificado."""
    print("\nCargando ambos datasets para construir el esquema unificado...")
    df1, nombre1 = cargar_dataframe("1")
    df2, nombre2 = cargar_dataframe("2")

    if df1 is None and df2 is None:
        print("No se pudo cargar ningún dataset.")
        return

    contexto = {}
    while True:
        print("\n--- Esquema estrella UNIFICADO (datos reales de ambos datasets) ---")
        print("1. Ver dim_tiempo (combinada)")
        print("2. Ver dim_artista (combinada)")
        print("3. Ver dim_track (combinada)")
        print("4. Ver hecho_streaming (combinado)")
        print("5. Ver duplicados (de cada dataset y entre ambos)")
        print("6. Limpiar ambos datasets")
        print("7. Ver uso de memoria de las tablas generadas")
        print("8. Volver al menú principal")
        op = input("Selecciona una opción: ").strip()

        if op == "1":
            contexto.setdefault("tiempo", construir_dim_tiempo_combinado(df1, df2))
            _mostrar_tabla(contexto["tiempo"], "dim_tiempo (unificada)")
        elif op == "2":
            contexto.setdefault("artista", construir_dim_artista_combinado(df1, df2))
            _mostrar_tabla(contexto["artista"], "dim_artista (unificada)")
        elif op == "3":
            contexto.setdefault("artista", construir_dim_artista_combinado(df1, df2))
            contexto.setdefault("track", construir_dim_track_combinado(df1, df2, contexto["artista"]))
            _mostrar_tabla(contexto["track"], "dim_track (unificada)")
        elif op == "4":
            contexto.setdefault("tiempo", construir_dim_tiempo_combinado(df1, df2))
            contexto.setdefault("artista", construir_dim_artista_combinado(df1, df2))
            contexto.setdefault("track", construir_dim_track_combinado(df1, df2, contexto["artista"]))
            fuentes = [(nombre1, df1), (nombre2, df2)]
            contexto.setdefault(
                "hecho",
                construir_hecho_streaming_combinado(fuentes, contexto["track"], contexto["tiempo"]),
            )
            _mostrar_tabla(contexto["hecho"], "hecho_streaming (unificado)")
        elif op == "5":
            mostrar_duplicados_combinados(df1, nombre1, df2, nombre2)
        elif op == "6":
            if df1 is not None:
                df1 = limpiar_dataframe(df1, nombre1)
                _cache_dataframes["1"] = df1
            if df2 is not None:
                df2 = limpiar_dataframe(df2, nombre2)
                _cache_dataframes["2"] = df2
            contexto.clear()  # las tablas ya construidas quedaron desactualizadas
            print("\nAmbos datasets limpios. Las tablas del esquema unificado se reconstruirán con los datos limpios.")
        elif op == "7":
            mostrar_memoria_tablas(
                {"dim_tiempo": contexto.get("tiempo"), "dim_artista": contexto.get("artista"),
                 "dim_track": contexto.get("track"), "hecho_streaming": contexto.get("hecho")},
                "esquema unificado",
            )
        elif op == "8":
            break
        else:
            print("Opción inválida.")


def menu_principal():
    while True:
        print("\n============================")
        print(" MENÚ DE DATASETS DE SPOTIFY")
        print("============================")
        for clave, info in DATASETS.items():
            print(f"{clave}. {info['nombre']}")
        print("3. Ver todos los datasets juntos (esquema unificado)")
        print("4. Ver uso de memoria del proyecto")
        print("5. Liberar memoria (vaciar caché de datasets)")
        print("6. Streaming en tiempo real (Kafka + Spark Structured Streaming)")
        print("7. Pipeline avanzado (API, ETL, Lakehouse, NumPy y Spark)")
        print("0. Salir")
        op = input("Selecciona una opción: ").strip()

        if op == "0":
            print("¡Hasta luego!")
            break
        elif op in DATASETS:
            menu_dataset(op)
        elif op == "3":
            menu_combinado()
        elif op == "4":
            mostrar_memoria_proyecto()
        elif op == "5":
            liberar_memoria()
        elif op == "6":
            import streaming
            streaming.menu_streaming(cargar_dataframe, buscar_columna, limpiar_artista, DATASETS)
        elif op == "7":
            import pipeline_avanzado
            pipeline_avanzado.menu()
        else:
            print("Opción inválida.")


if __name__ == "__main__":
    menu_principal()