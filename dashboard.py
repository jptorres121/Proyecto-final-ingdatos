"""Dashboard interactivo para el proyecto de Ingeniería de Datos Spotify.
Ejecutar: streamlit run dashboard.py

Cambios de rendimiento respecto a la versión anterior:
- Las fuentes pesadas (Data Lake, Kaggle, archivos subidos) se cargan UNA vez y se reutilizan.
- Los cálculos pesados (nulos, únicos, duplicados, conteos) se cachean por fuente.
- Solo se dibuja la sección activa (antes se dibujaban las 4 pestañas en cada interacción).
- La fuente API iTunes se consulta sola al abrir la página (ya no hay que pulsar el botón).
- Kaggle se carga sin pedir nada por consola (antes input() dejaba la página colgada).
"""
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

# `use_container_width` está obsoleto en Streamlit reciente; `width="stretch"` no existe en el antiguo.
try:
    W = {"width": "stretch"} if tuple(int(x) for x in st.__version__.split(".")[:2]) >= (1, 50) \
        else {"use_container_width": True}
except Exception:
    W = {"use_container_width": True}
LAKE = BASE / "data_lake"
ITUNES_URL = "https://itunes.apple.com/search"

st.set_page_config(page_title="Spotify | Ingeniería de Datos", page_icon="🎧",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
.main {background-color:#101318}
.block-container {padding-top:1.2rem; padding-bottom:1.5rem; max-width:1400px}
[data-testid="stMetric"] {background:#1b2027; border:1px solid #2a3038; padding:10px 14px; border-radius:9px}
section[data-testid="stSidebar"] {background:#15191f}
h1,h2,h3 {letter-spacing:-.2px}
hr {opacity:.25}
</style>
""", unsafe_allow_html=True)

st.title("Spotify · Análisis de datos musicales")
st.caption("Explora el catálogo, revisa la calidad de los datos y consulta indicadores clave.")

SOURCES = [
    "API iTunes · Búsqueda en vivo",
    "Data Lake · Silver",
    "Data Lake · Bronze",
    "Dataset Kaggle 1921–2020",
    "Dataset Spotify200",
    "API iTunes · Bronze",
    "Cargar archivo CSV / Parquet / JSON",
]
SECCIONES = ["📊 Panel analítico", "🎵 Catálogo musical", "🧹 Calidad de datos", "🗃️ Modelo y datos"]
DEFAULT_TERM, DEFAULT_LIMIT = "Coldplay", 50


# ---------------------------------------------------------------- utilidades
def find_col(df, candidates):
    lower = {str(c).lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def numeric_col(df, candidates):
    c = find_col(df, candidates)
    if c and pd.to_numeric(df[c], errors="coerce").notna().any():
        return c
    return None


def importar_ingdatos():
    """ingdatos hace sys.exit() si falta kagglehub; aquí lo convertimos en un error normal."""
    try:
        import ingdatos
        return ingdatos
    except SystemExit:
        raise RuntimeError("Falta la librería kagglehub: pip install \"kagglehub[pandas-datasets]\"")


def firma_carpeta(carpeta: Path, patrones):
    """Cambia solo si cambian los archivos (nombre, tamaño, fecha) → permite cachear la lectura."""
    archivos = sorted(f for p in patrones for f in carpeta.glob(p)) if carpeta.exists() else []
    return tuple((f.name, f.stat().st_size, int(f.stat().st_mtime)) for f in archivos)


# ------------------------------------------------------------- carga (cacheada)
@st.cache_resource(show_spinner=False)
def leer_lake(carpeta: str, patrones: tuple, firma: tuple):
    frames = []
    for f in sorted(f for p in patrones for f in Path(carpeta).glob(p)):
        try:
            frames.append(pd.read_parquet(f) if f.suffix == ".parquet" else pd.read_csv(f))
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else None


@st.cache_resource(show_spinner=False)
def load_uploaded(_file_bytes, name, size):
    import io
    if name.lower().endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(_file_bytes))
    if name.lower().endswith(".json"):
        try:
            return pd.read_json(io.BytesIO(_file_bytes), lines=True)
        except ValueError:
            return pd.read_json(io.BytesIO(_file_bytes))
    return pd.read_csv(io.BytesIO(_file_bytes))


@st.cache_data(ttl=600, show_spinner=False)
def buscar_itunes_api(termino, limite):
    import requests
    response = requests.get(ITUNES_URL, params={"term": termino, "entity": "song", "limit": limite},
                            timeout=(5, 15))
    response.raise_for_status()
    results = response.json().get("results", [])
    return pd.DataFrame([{
        "track_id_api": r.get("trackId"), "titulo_track": r.get("trackName"),
        "nombre_artista": r.get("artistName"), "album": r.get("collectionName"),
        "genero_musical": r.get("primaryGenreName"), "duracion_ms": r.get("trackTimeMillis"),
        "fecha_lanzamiento": r.get("releaseDate"), "url_preview": r.get("previewUrl"),
        "url_catalogo": r.get("trackViewUrl"), "artwork_url": r.get("artworkUrl100"),
        "fuente": "iTunes Search API"
    } for r in results])


def traer_api(termino, limite):
    """Consulta la API y guarda el resultado en la sesión. Devuelve True si funcionó."""
    try:
        with st.spinner("Consultando catálogo musical..."):
            st.session_state["itunes_live_df"] = buscar_itunes_api(termino, limite)
        st.session_state.update(itunes_live_term=termino, itunes_live_limit=limite,
                                itunes_live_at=datetime.now().strftime("%H:%M:%S"))
        st.session_state.pop("itunes_error", None)
        return True
    except Exception as exc:
        st.session_state["itunes_error"] = str(exc)
        return False


def cargar_kaggle(clave):
    ing = importar_ingdatos()
    nombre = ing.DATASETS[clave]["nombre"]
    ya_en_memoria = clave in ing._cache_dataframes
    with st.spinner(f"Leyendo «{nombre}»…" if ya_en_memoria else
                    f"Descargando/leyendo «{nombre}» (solo la primera vez, puede tardar)…"):
        df, _ = ing.cargar_dataframe(clave, interactivo=False)
    origen = f"Kaggle · {ing.DATASETS[clave]['handle']}"
    archivo = ing._cache_rutas.get(f"archivo_{clave}")
    if archivo:
        origen += f" · {Path(archivo).name}"
    return df, origen


@st.cache_resource(show_spinner=False)
def construir_unificado():
    """Esquema estrella unificado: fusiona y deduplica Kaggle 1921–2020 + Spotify200."""
    ing = importar_ingdatos()
    df1, n1 = ing.cargar_dataframe("1", interactivo=False)
    df2, n2 = ing.cargar_dataframe("2", interactivo=False)
    if df1 is None and df2 is None:
        raise RuntimeError("No se pudo cargar ninguno de los dos datasets de Kaggle.")
    tiempo = ing.construir_dim_tiempo_combinado(df1, df2)
    artista = ing.construir_dim_artista_combinado(df1, df2)
    track = ing.construir_dim_track_combinado(df1, df2, artista)
    hecho = ing.construir_hecho_streaming_combinado([(n1, df1), (n2, df2)], track, tiempo)
    tablas = {"dim_tiempo": tiempo, "dim_artista": artista, "dim_track": track, "hecho_streaming": hecho}
    fuentes = [(n1, 0 if df1 is None else len(df1)), (n2, 0 if df2 is None else len(df2))]
    return {"tablas": {k: v for k, v in tablas.items() if v is not None}, "fuentes": fuentes}


# ------------------------------------------------------ cálculos pesados (cacheados)
# `_df` con guion bajo = Streamlit no lo hashea; la clave `k` identifica la fuente.
@st.cache_data(show_spinner=False)
def detectar_columnas(_df, k):
    return {
        "artist": find_col(_df, ["nombre_artista", "artists", "artist", "artist_name"]),
        "track": find_col(_df, ["titulo_track", "name", "track_name", "title", "cancion"]),
        "genre": find_col(_df, ["genero_musical", "genre", "track_genre", "genero"]),
        "streams": numeric_col(_df, ["streams", "stream", "playcount", "cantidad_reproducciones", "reproducciones"]),
        "duration": numeric_col(_df, ["duracion_ms", "duration_ms", "duration"]),
        "popularity": numeric_col(_df, ["popularity", "popularidad"]),
        "year": find_col(_df, ["year", "release_year", "anio", "año"]),
    }


@st.cache_data(show_spinner=False)
def kpis(_df, k, artist, genre):
    return {
        "artistas": int(_df[artist].nunique()) if artist else None,
        "generos": int(_df[genre].nunique()) if genre else None,
        "nulos": int(_df.isna().sum().sum()),
    }


@st.cache_data(show_spinner=False)
def conteo(_df, k, col, n):
    return _df[col].fillna("Sin dato").astype(str).value_counts().head(n)


@st.cache_data(show_spinner=False)
def suma_por_artista(_df, k, artist, metric, n=8):
    m = pd.to_numeric(_df[metric], errors="coerce")
    return m.groupby(_df[artist]).sum().nlargest(n).sort_values()


@st.cache_data(show_spinner=False)
def hist_duracion(_df, k, col):
    vals = pd.to_numeric(_df[col], errors="coerce").dropna()
    vals = vals / 60000 if len(vals) and vals.median() > 100 else vals
    hist = pd.cut(vals, bins=20).value_counts().sort_index()
    hist.index = hist.index.astype(str)
    return hist


@st.cache_data(show_spinner=False)
def perfil_calidad(_df, k):
    quality = pd.DataFrame({
        "columna": _df.columns,
        "tipo": [str(_df[c].dtype) for c in _df.columns],
        "nulos": [int(_df[c].isna().sum()) for c in _df.columns],
        "% nulos": [round(float(_df[c].isna().mean() * 100), 2) for c in _df.columns],
        "valores_unicos": [int(_df[c].nunique(dropna=True)) for c in _df.columns],
    })
    try:
        duplicadas = int(_df.duplicated().sum())
    except TypeError:  # columnas con listas/dicts no se pueden comparar
        duplicadas = int(_df.astype(str).duplicated().sum())
    return quality, duplicadas


@st.cache_data(show_spinner=False)
def memoria_mb(_df, k):
    return float(_df.memory_usage(deep=True).sum() / 1024 ** 2)


@st.cache_data(show_spinner=False)
def csv_bytes(_df, k):
    return _df.to_csv(index=False).encode("utf-8-sig")


# ------------------------------------------------------------------- sidebar
ICONOS = ["🔎", "🥈", "🥉", "📀", "📈", "🍎", "📁"]


def _elegir(src):
    """Selecciona un dataset. Si estabas en «unificados» vuelves al dataset; en el laboratorio te quedas."""
    st.session_state["source"] = src
    if st.session_state.get("vista") == "unificado":
        st.session_state["vista"] = "dataset"


st.session_state.setdefault("vista", "dataset")
st.session_state.setdefault("source", SOURCES[0])

with st.sidebar:
    st.header("Fuentes de datos")
    for i, nombre in enumerate(SOURCES):
        activo = nombre == st.session_state["source"] and st.session_state["vista"] != "unificado"
        st.button(f"{ICONOS[i]} {nombre}", key=f"src_{i}", on_click=_elegir, args=(nombre,),
                  type="primary" if activo else "secondary", **W)
    source = st.session_state["source"]
    uploaded = None
    if source == SOURCES[0]:
        with st.form("itunes_live_form"):
            api_query = st.text_input("Artista, canción o término", value=DEFAULT_TERM,
                                      placeholder="Ej. Shakira, rock, jazz...")
            api_limit = st.slider("Cantidad de resultados", min_value=10, max_value=200,
                                  value=DEFAULT_LIMIT, step=10)
            api_submit = st.form_submit_button("🔎 Obtener datos desde API", **W)
        if api_submit and api_query.strip():
            if st.session_state["vista"] == "unificado":
                st.session_state["vista"] = "dataset"
            traer_api(api_query.strip(), api_limit)
        elif "itunes_live_df" not in st.session_state and "itunes_error" not in st.session_state:
            traer_api(DEFAULT_TERM, DEFAULT_LIMIT)  # carga automática al abrir la página
        if "itunes_error" in st.session_state:
            st.warning(f"No se pudo consultar la API: {st.session_state['itunes_error']}")
    elif source == SOURCES[6]:
        uploaded = st.file_uploader("Selecciona un archivo", type=["csv", "parquet", "json"])

    st.divider()
    st.subheader("Navegación")
    btn_ubicacion = st.button("📍 ¿En qué dataset estoy?", **W)
    if st.button("🔗 Datasets unificados", **W,
                 type="primary" if st.session_state["vista"] == "unificado" else "secondary"):
        st.session_state["vista"] = "unificado"
        st.rerun()

    if st.button("🎓 Laboratorio de temas de clase", **W,
                 type="primary" if st.session_state["vista"] == "laboratorio" else "secondary"):
        st.session_state["vista"] = "laboratorio"
        st.rerun()

    st.divider()
    st.subheader("Operaciones")
    run_etl = st.button("⚙️ Ejecutar ETL Bronze → Silver → Gold", **W)
    run_model = st.button("⭐ Construir modelos dimensionales", **W)
    st.caption("Las operaciones de procesamiento guardan sus resultados en data_lake/.")
    flash = st.session_state.pop("flash", None)
    if flash:
        (st.success if flash[0] == "ok" else st.error)(flash[1])

vista = st.session_state["vista"]


# ------------------------------------------------- carga de la fuente seleccionada
def cargar_fuente(source, uploaded):
    """Devuelve (df, meta, error, aviso)."""
    meta = {"etiqueta": source, "tipo": "—", "capa": "—", "origen": "—", "clave": source}
    df, error, aviso = None, None, None
    try:
        if source == SOURCES[0]:
            df = st.session_state.get("itunes_live_df")
            term = st.session_state.get("itunes_live_term", "consulta")
            lim = st.session_state.get("itunes_live_limit", "")
            meta.update(etiqueta=f"iTunes API · {term}", tipo="API en vivo", capa="Externa (no guardada en el lake)",
                        origen=f"{ITUNES_URL}?term={term}&entity=song&limit={lim}",
                        clave=f"api|{term}|{lim}|{st.session_state.get('itunes_live_at')}",
                        actualizado=st.session_state.get("itunes_live_at"))
        elif source == SOURCES[6]:
            if uploaded is not None:
                df = load_uploaded(uploaded.getvalue(), uploaded.name, uploaded.size)
                meta.update(etiqueta=f"Archivo · {uploaded.name}", tipo="Archivo cargado", capa="Externa",
                            origen=uploaded.name, clave=f"up|{uploaded.name}|{uploaded.size}")
        elif source in (SOURCES[1], SOURCES[2], SOURCES[5]):
            carpeta = LAKE / ("silver" if source == SOURCES[1] else "bronze")
            patrones = (("itunes_*.parquet", "itunes_*.csv") if source == SOURCES[5]
                        else ("*.parquet", "*.csv"))
            firma = firma_carpeta(carpeta, patrones)
            meta.update(tipo="Data Lake", capa="Silver (limpia)" if source == SOURCES[1] else "Bronze (cruda)",
                        origen=str(carpeta.relative_to(BASE)), clave=f"lake|{carpeta.name}|{patrones[0]}|{hash(firma)}")
            if firma:
                df = leer_lake(str(carpeta), patrones, firma)
            else:
                aviso = (f"Todavía no hay datos en `{carpeta.relative_to(BASE)}`. Elige **API iTunes · Búsqueda en vivo** "
                         "(se carga sola) y pulsa **⚙️ Ejecutar ETL** para generar las capas Bronze/Silver/Gold.")
        elif source in (SOURCES[3], SOURCES[4]):
            clave = "1" if source == SOURCES[3] else "2"
            df, origen = cargar_kaggle(clave)
            meta.update(tipo="Dataset Kaggle", capa="Dataset base (crudo)", origen=origen,
                        clave=f"kaggle|{clave}|{0 if df is None else len(df)}")
    except Exception as exc:
        error = str(exc)
    return df, meta, error, aviso


if vista in ("dataset", "laboratorio"):
    df, meta, error, aviso = cargar_fuente(source, uploaded)
else:  # no cargamos el dataset actual si estamos viendo los unificados (ahorra tiempo y memoria)
    df, meta, error, aviso = None, {"etiqueta": "Datasets unificados", "clave": "unificado"}, None, None

# ----------------------------------------------------------------- operaciones
if run_etl:
    try:
        import pipeline_avanzado as pipeline
        usar_df = df if (vista == "dataset" and source != SOURCES[3] and source != SOURCES[4]) else None
        silver, gold = pipeline.ejecutar_etl(usar_df)
        st.session_state["flash"] = ("ok", f"ETL finalizado: {len(silver):,} filas Silver.")
    except Exception as exc:
        st.session_state["flash"] = ("err", f"No se pudo ejecutar ETL: {exc}")
    st.rerun()

if run_model:
    try:
        import pipeline_avanzado as pipeline
        base_df = df if df is not None else pipeline._leer_tabla(pipeline.SILVER)
        tablas = pipeline.construir_modelos_dimensionales(base_df)
        st.session_state["flash"] = ("ok", f"Se generaron {len(tablas)} tablas dimensionales.")
    except Exception as exc:
        st.session_state["flash"] = ("err", f"No se pudieron construir los modelos: {exc}")
    st.rerun()


# ---------------------------------------------------------- botón: ¿dónde estoy?
@st.dialog("📍 ¿En qué dataset estoy?")
def dialogo_ubicacion(vista, meta, df):
    if vista == "unificado":
        st.markdown("### 🔗 Datasets unificados")
        st.write("Estás viendo el **esquema estrella unificado**: Kaggle 1921–2020 + Spotify200 "
                 "fusionados y sin duplicados entre sí.")
        st.caption("Usa el menú «Fuentes de datos» para volver a un dataset individual.")
        return
    st.markdown(f"### {meta['etiqueta']}")
    st.write(f"**Tipo:** {meta.get('tipo', '—')}")
    st.write(f"**Capa:** {meta.get('capa', '—')}")
    st.write(f"**Origen:** `{meta.get('origen', '—')}`")
    if meta.get("actualizado"):
        st.write(f"**Consultado a las:** {meta['actualizado']}")
    if df is None:
        st.warning("Esta fuente no tiene datos cargados todavía.")
        return
    cols = detectar_columnas(df, meta["clave"])
    a, b, c = st.columns(3)
    a.metric("Registros", f"{len(df):,}")
    b.metric("Columnas", len(df.columns))
    c.metric("Memoria", f"{memoria_mb(df, meta['clave']):.1f} MB")
    nombres = {"artist": "Artista", "track": "Canción", "genre": "Género", "streams": "Reproducciones",
               "duration": "Duración", "popularity": "Popularidad", "year": "Año"}
    st.write("**Columnas reconocidas:**")
    st.code("\n".join(f"{nombres[k]:<15} → {v}" for k, v in cols.items() if v) or "Ninguna", language=None)


if btn_ubicacion:
    dialogo_ubicacion(vista, meta, df)


# -------------------------------------------------------- vista: unificados
def vista_unificada():
    st.subheader("🔗 Datasets unificados")
    st.caption("Esquema estrella con Kaggle 1921–2020 y Spotify200 fusionados; los registros repetidos entre "
               "ambos se deduplican por título + artista.")
    try:
        with st.spinner("Construyendo el esquema unificado (solo la primera vez, puede tardar)…"):
            res = construir_unificado()
    except Exception as exc:
        st.error(f"No fue posible construir los datasets unificados: {exc}")
        st.info("Necesitas conexión a internet (y credenciales de Kaggle si tu configuración las pide) "
                "para descargar los dos datasets la primera vez.")
        return
    tablas, fuentes = res["tablas"], res["fuentes"]

    st.markdown("**Fuentes combinadas**")
    f1, f2, f3 = st.columns(3)
    f1.metric(fuentes[0][0], f"{fuentes[0][1]:,} filas")
    f2.metric(fuentes[1][0], f"{fuentes[1][1]:,} filas")
    if "dim_track" in tablas:
        f3.metric("Canciones únicas (dim_track)", f"{len(tablas['dim_track']):,}")

    cols = st.columns(len(tablas))
    for col, (nombre, t) in zip(cols, tablas.items()):
        col.metric(nombre, f"{len(t):,}", f"{len(t.columns)} columnas", delta_color="off")

    elegida = st.radio("Tabla", list(tablas), horizontal=True, key="tabla_unificada")
    t = tablas[elegida]
    st.dataframe(t.head(500), **W, height=380)
    st.caption(f"Mostrando hasta 500 de {len(t):,} filas de **{elegida}**.")

    destino = LAKE / "gold" / "unificado"
    if st.button(f"💾 Guardar las {len(tablas)} tablas en {destino.relative_to(BASE)}/"):
        try:
            import pipeline_avanzado as pipeline
            for nombre, tabla in tablas.items():
                pipeline._guardar_tabla(tabla, destino / nombre)
            st.success(f"Tablas guardadas en {destino.relative_to(BASE)}/")
        except Exception as exc:
            st.error(f"No se pudieron guardar: {exc}")


@st.cache_data(show_spinner=False)
def catalogo_lab(_df, k):
    import lab_batch
    return lab_batch.catalogo(_df)


def vista_laboratorio():
    import lab_batch
    import lab_stream
    st.subheader("🎓 Laboratorio de temas vistos en clase")
    if error:
        st.error(f"No fue posible cargar la fuente: {error}")
    cat, notas = catalogo_lab(df, meta["clave"])
    st.caption(f"📍 Las demos usan: **{meta['etiqueta']}** (cambia de dataset con los botones de la izquierda).")
    for n in notas:
        st.caption(f"ℹ️ {n}")
    ctx = {"lake": LAKE, "base": BASE}
    temas = {
        "1 · OLTP y OLAP": lab_batch.tema_oltp_olap,
        "2 · Data Warehouse, Data Lake y Lakehouse": lab_batch.tema_dw_lake,
        "3 · Modelos dimensionales: estrella y copo de nieve": lab_batch.tema_dimensional,
        "4 · Python, NumPy, Pandas y operaciones con DataFrames": lab_batch.tema_numpy_pandas,
        "5 · APIs y extracción de datos": lab_batch.tema_apis,
        "6 · Apache Spark y procesamiento distribuido": lab_batch.tema_spark,
        "7 · Transformaciones, acciones, batch y streaming": lab_batch.tema_lazy_batch,
        "8 · Kafka: topics, brokers, productores y consumidores": lab_stream.tema_kafka,
        "9 · Spark Structured Streaming": lab_stream.tema_structured,
        "10 · Event Time, Processing Time y Watermarks": lab_stream.tema_tiempo_watermark,
        "11 · Ventanas de tiempo y eventos tardíos": lab_stream.tema_ventanas,
        "12 · Tolerancia a fallos y recuperación": lab_stream.tema_tolerancia,
        "13 · Apache Flink y Kafka Streams": lab_stream.tema_flink,
        "14 · ETL, ELT y construcción de pipelines": lab_batch.tema_etl_elt,
    }
    tema = st.selectbox("Tema", list(temas), key="lab_tema")
    st.divider()
    temas[tema](cat, ctx)


if vista == "unificado":
    vista_unificada()
    st.stop()

if vista == "laboratorio":
    vista_laboratorio()
    st.stop()

# ----------------------------------------------------------------- vista: dataset
if error:
    st.error(f"No fue posible cargar la fuente: {error}")
if aviso:
    st.info(aviso)
if df is None:
    if source == SOURCES[0] and "itunes_error" in st.session_state:
        st.warning("No hubo respuesta de la API de iTunes (revisa tu conexión). "
                   "Mientras tanto puedes usar las fuentes locales: Data Lake, Kaggle o cargar un archivo.")
    elif not aviso and not error:
        st.info("Busca un artista, canción o género en la fuente API iTunes del panel izquierdo y presiona "
                "Obtener datos. No necesitas cargar archivos CSV para comenzar.")
    st.markdown("### ¿Qué puedes visualizar?")
    c1, c2, c3 = st.columns(3)
    c1.info("**Análisis exploratorio**\n\nRegistros, columnas, valores nulos y distribución.")
    c2.info("**Analítica musical**\n\nArtistas, géneros, popularidad y duración.")
    c3.info("**Ingeniería de datos**\n\nCapas Bronze/Silver/Gold y modelos dimensionales.")
    st.stop()

k = meta["clave"]
st.caption(f"📍 Dataset actual: **{meta['etiqueta']}**  ·  {len(df):,} registros  ·  {len(df.columns)} columnas")

cols = detectar_columnas(df, k)
artist, track, genre = cols["artist"], cols["track"], cols["genre"]
streams, duration, popularity, year = cols["streams"], cols["duration"], cols["popularity"], cols["year"]

kp = kpis(df, k, artist, genre)
k1, k2, k3, k4 = st.columns(4)
k1.metric("Registros", f"{len(df):,}")
k2.metric("Artistas", f"{kp['artistas']:,}" if kp["artistas"] is not None else "N/D")
k3.metric("Géneros", f"{kp['generos']:,}" if kp["generos"] is not None else "N/D")
k4.metric("Valores nulos", f"{kp['nulos']:,}")

# Solo se dibuja la sección elegida (las pestañas st.tabs ejecutaban las 4 en cada interacción)
seccion = st.radio("Sección", SECCIONES, horizontal=True, label_visibility="collapsed", key="seccion")

if seccion == SECCIONES[0]:
    st.subheader("Resumen del conjunto de datos")
    left, right = st.columns(2)
    with left:
        if artist:
            st.markdown("**Artistas con más registros**")
            st.bar_chart(conteo(df, k, artist, 8).sort_values(), horizontal=True, height=280)
        elif genre:
            st.markdown("**Registros por género**")
            st.bar_chart(conteo(df, k, genre, 15))
        else:
            st.info("No se encontró una columna de artista o género reconocible.")
    with right:
        if genre:
            st.markdown("**Géneros más frecuentes**")
            st.bar_chart(conteo(df, k, genre, 10), height=360)
        elif streams:
            st.markdown("**Distribución de reproducciones**")
            st.line_chart(pd.to_numeric(df[streams], errors="coerce").dropna().head(5000).reset_index(drop=True))
        else:
            st.markdown("**Vista de columnas numéricas**")
            nums = df.select_dtypes(include="number")
            if not nums.empty:
                st.line_chart(nums.iloc[:, :4].dropna().head(300))
            else:
                st.info("La fuente no contiene columnas numéricas para graficar.")
    if streams:
        st.markdown("**Reproducciones por artista (principales)**")
        if artist:
            st.bar_chart(suma_por_artista(df, k, artist, streams), horizontal=True, height=280)
        else:
            st.line_chart(pd.to_numeric(df[streams], errors="coerce").dropna().head(5000).reset_index(drop=True))
    if duration:
        st.markdown("**Distribución de duración (minutos)**")
        st.bar_chart(hist_duracion(df, k, duration), height=220)

elif seccion == SECCIONES[1]:
    st.subheader("Exploración del catálogo")
    if track:
        search = st.text_input("Buscar canción", placeholder="Escribe parte del título...")
        view = df
        if search:
            view = view[view[track].astype(str).str.contains(search, case=False, na=False, regex=False)]
        sel = [c for c in [track, artist, genre, streams, duration, popularity, year] if c and c in view.columns]
        st.dataframe(view[sel].head(300) if sel else view.head(300), **W, height=360)
        st.caption(f"Mostrando hasta 300 de {len(view):,} registros filtrados.")
    else:
        st.dataframe(df.head(300), **W, height=360)
    if popularity and streams:
        st.markdown("**Popularidad frente a reproducciones**")
        scatter = df[[popularity, streams]].head(3000).apply(pd.to_numeric, errors="coerce").dropna()
        st.scatter_chart(scatter, x=popularity, y=streams)

elif seccion == SECCIONES[2]:
    st.subheader("Perfil de calidad")
    with st.spinner("Calculando perfil de calidad (se guarda en caché)…"):
        quality, duplicadas = perfil_calidad(df, k)
    st.dataframe(quality, **W, hide_index=True)
    q1, q2, q3 = st.columns(3)
    q1.metric("Filas duplicadas", f"{duplicadas:,}")
    q2.metric("Celdas vacías", f"{kp['nulos']:,}")
    q3.metric("Columnas", len(df.columns))
    st.markdown("**Porcentaje de valores nulos por columna**")
    st.bar_chart(quality.set_index("columna")["% nulos"])

else:
    st.subheader("Estructura del conjunto de datos")
    st.write("**Columnas disponibles**")
    st.code(", ".join(map(str, df.columns)))
    st.write("**Tipos de datos**")
    st.dataframe(df.dtypes.astype(str).rename("tipo").to_frame(), **W)
    st.write("**Vista previa**")
    st.dataframe(df.head(20), **W)
    if len(df) <= 50_000:
        st.download_button("⬇️ Descargar datos visualizados (CSV)", csv_bytes(df, k),
                           file_name="spotify_datos_dashboard.csv", mime="text/csv")
    else:  # evita generar un CSV enorme en cada interacción
        if st.button(f"Preparar CSV completo ({len(df):,} filas)"):
            st.session_state["csv_listo"] = k
        if st.session_state.get("csv_listo") == k:
            st.download_button("⬇️ Descargar datos visualizados (CSV)", csv_bytes(df, k),
                               file_name="spotify_datos_dashboard.csv", mime="text/csv")

st.caption("Dashboard académico: las métricas dependen de las columnas disponibles en la fuente seleccionada. "
           "Los datos de iTunes son metadatos de catálogo y no reproducciones reales de Spotify.")
