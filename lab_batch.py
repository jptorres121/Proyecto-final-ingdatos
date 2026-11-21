"""Laboratorio de temas de clase — parte 1 (datos y procesamiento batch).

Cada función `tema_*` recibe `cat` (catálogo normalizado del dataset elegido) y `ctx`
(rutas del proyecto) y dibuja una demo interactiva con Streamlit.
Todo corre sin servicios externos, salvo lo que se indique (API iTunes, Spark real).
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st

# `use_container_width` está obsoleto en Streamlit reciente; `width="stretch"` no existe en el antiguo.
try:
    _NUEVO = tuple(int(x) for x in st.__version__.split(".")[:2]) >= (1, 50)
except Exception:
    _NUEVO = False
W = {"width": "stretch"} if _NUEVO else {"use_container_width": True}

GENEROS = ["Pop", "Rock", "Hip-Hop", "Electrónica", "Latino", "Jazz", "Clásica", "R&B"]
ITUNES_URL = "https://itunes.apple.com/search"


def _t():
    return time.perf_counter()


# ------------------------------------------------------------------ datos base
def _col(df, cands):
    lower = {str(c).lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def _limpiar_artista(v):
    if pd.isna(v):
        return v
    t = str(v).strip()
    if t.startswith("[") and t.endswith("]"):
        t = t.strip("[]").split(",")[0].strip().strip("'").strip('"')
    return t


def datos_demo(n=2000):
    rng = np.random.default_rng(7)
    fechas = pd.to_datetime("2018-01-01") + pd.to_timedelta(rng.integers(0, 365 * 7, n), unit="D")
    return pd.DataFrame({
        "id": np.arange(1, n + 1),
        "titulo": [f"Canción {i}" for i in range(n)],
        "artista": rng.choice([f"Artista {i:02d}" for i in range(40)], n),
        "genero": rng.choice(GENEROS, n),
        "duracion_ms": rng.normal(215000, 45000, n).clip(60000, 600000).astype(int),
        "fecha": fechas.to_numpy(),
        "reproducciones": rng.lognormal(10, 1.5, n).astype(int),
    })


def catalogo(df, max_rows=20000):
    """Normaliza cualquier fuente a: id, titulo, artista, genero, duracion_ms, fecha, reproducciones.
    Lo que la fuente no trae se simula (y se avisa) para que todas las demos funcionen."""
    if df is None or len(df) == 0:
        return datos_demo(), ["No hay dataset cargado: las demos usan datos sintéticos de demostración."]
    m = df.sample(max_rows, random_state=1) if len(df) > max_rows else df
    ct = _col(m, ["titulo_track", "name", "track_name", "title", "cancion"])
    ca = _col(m, ["nombre_artista", "artists", "artist", "artist_name"])
    if not ct or not ca:
        return datos_demo(), ["La fuente actual no tiene título y artista: las demos usan datos sintéticos."]
    notas, n, rng = [], len(m), np.random.default_rng(11)
    if len(df) > max_rows:
        notas.append(f"Se usa una muestra de {max_rows:,} de {len(df):,} filas para que las demos sean rápidas.")
    cat = pd.DataFrame({"id": np.arange(1, n + 1)})
    cat["titulo"] = m[ct].astype(str).to_numpy()
    cat["artista"] = m[ca].map(_limpiar_artista).fillna("desconocido").astype(str).to_numpy()

    cf = _col(m, ["fecha_lanzamiento", "release_date", "date", "week", "chart_week", "fecha"])
    fecha = pd.to_datetime(m[cf], errors="coerce", utc=True).dt.tz_localize(None) if cf else None
    if fecha is None or fecha.isna().all():
        fecha = pd.Series(pd.to_datetime("2020-01-01") + pd.to_timedelta(rng.integers(0, 1500, n), unit="D"))
        notas.append("La fuente no trae fechas utilizables: se simularon.")
    else:
        fecha = fecha.fillna(fecha.dropna().iloc[0])
    cat["fecha"] = fecha.to_numpy()

    cg = _col(m, ["genero_musical", "genre", "track_genre", "genero"])
    if cg:
        cat["genero"] = m[cg].fillna("sin_genero").astype(str).to_numpy()
    else:
        cat["genero"] = ((cat["fecha"].dt.year // 10 * 10).astype(int).astype(str) + "s").to_numpy()
        notas.append("La fuente no trae género: se agrupa por década de la fecha.")

    cd = _col(m, ["duracion_ms", "duration_ms", "duration"])
    dur = pd.to_numeric(m[cd], errors="coerce") if cd else None
    if dur is None or dur.notna().sum() == 0:
        cat["duracion_ms"] = rng.normal(215000, 45000, n).clip(60000, 600000).astype(int)
        notas.append("La fuente no trae duración: se simuló.")
    else:
        cat["duracion_ms"] = dur.fillna(dur.median()).astype(int).to_numpy()

    cs = _col(m, ["streams", "stream", "playcount", "cantidad_reproducciones", "reproducciones"])
    st_ = pd.to_numeric(m[cs], errors="coerce") if cs else None
    if st_ is None or st_.notna().sum() == 0:
        cat["reproducciones"] = rng.lognormal(10, 1.5, n).astype(int)
        notas.append("La fuente no trae reproducciones: se simularon (iTunes no las publica).")
    else:
        cat["reproducciones"] = st_.fillna(0).astype(int).to_numpy()
    return cat, notas


def _renombrar_pipeline(cat):
    return cat.rename(columns={"titulo": "titulo_track", "artista": "nombre_artista",
                               "genero": "genero_musical", "fecha": "fecha_lanzamiento"})


# ------------------------------------------------------------ 1. OLTP vs OLAP
def tema_oltp_olap(cat, ctx):
    st.markdown(
        "**OLTP** (Online Transaction Processing) atiende muchas operaciones pequeñas y concurrentes "
        "(leer/actualizar *un* registro) con garantías ACID. **OLAP** (Online Analytical Processing) "
        "recorre muchos registros para agregar y analizar. Aquí se ejecutan ambos tipos sobre tu dataset.")
    n_ops = st.slider("Operaciones OLTP (SELECT + UPDATE por id)", 100, 5000, 1000, 100)
    con = sqlite3.connect(":memory:")
    cat.to_sql("tracks", con, index=False)
    con.execute("CREATE UNIQUE INDEX idx_tracks_id ON tracks(id)")
    ids = np.random.default_rng(1).integers(1, len(cat) + 1, n_ops)

    t0 = _t()
    for i in ids:
        con.execute("SELECT * FROM tracks WHERE id=?", (int(i),)).fetchone()
        con.execute("UPDATE tracks SET reproducciones = reproducciones + 1 WHERE id=?", (int(i),))
    con.commit()
    t_oltp = _t() - t0

    antes = con.execute("SELECT reproducciones FROM tracks WHERE id=1").fetchone()[0]
    con.execute("UPDATE tracks SET reproducciones = reproducciones + 1000000 WHERE id=1")
    dentro = con.execute("SELECT reproducciones FROM tracks WHERE id=1").fetchone()[0]
    con.rollback()
    despues = con.execute("SELECT reproducciones FROM tracks WHERE id=1").fetchone()[0]

    sql = ("SELECT genero, COUNT(*) AS canciones, ROUND(AVG(duracion_ms)/60000.0,2) AS duracion_min, "
           "SUM(reproducciones) AS reproducciones FROM tracks GROUP BY genero ORDER BY reproducciones DESC")
    t0 = _t()
    olap_sql = pd.read_sql_query(sql, con)
    t_sql = _t() - t0
    t0 = _t()
    olap_pd = (cat.groupby("genero").agg(canciones=("id", "count"), duracion_min=("duracion_ms", "mean"),
                                         reproducciones=("reproducciones", "sum")))
    t_pd = _t() - t0

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 🧾 OLTP — transacciones puntuales")
        a, b = st.columns(2)
        a.metric("Operaciones/segundo", f"{2 * n_ops / t_oltp:,.0f}")
        b.metric("Latencia media", f"{t_oltp / (2 * n_ops) * 1000:.3f} ms")
        st.caption("Lecturas y escrituras por clave primaria con índice.")
        st.code(f"Transacción con ROLLBACK\nantes     = {antes:,}\ndentro    = {dentro:,}  (UPDATE sin confirmar)\n"
                f"después   = {despues:,}  (ROLLBACK → vuelve al valor original)", language=None)
    with c2:
        st.markdown("#### 📊 OLAP — análisis agregado")
        a, b = st.columns(2)
        a.metric("SQL (fila a fila)", f"{t_sql * 1000:.1f} ms")
        b.metric("Pandas (columnar)", f"{t_pd * 1000:.1f} ms")
        st.caption(f"Escanea las {len(cat):,} filas para agrupar por género.")
        st.dataframe(olap_sql, hide_index=True, **W)
    st.markdown("#### Comparación")
    st.dataframe(pd.DataFrame({
        "Aspecto": ["Objetivo", "Consulta típica", "Volumen por consulta", "Escrituras", "Modelo de datos",
                    "Almacenamiento", "Usuarios", "En este proyecto"],
        "OLTP": ["Operar el negocio", "Buscar/actualizar 1 registro", "Pocas filas", "Muchas, concurrentes",
                 "Normalizado (3FN)", "Por filas", "Aplicaciones, clientes", "Catálogo/consulta por id"],
        "OLAP": ["Analizar el negocio", "Agregar, agrupar, comparar", "Millones de filas", "Cargas por lotes",
                 "Dimensional (estrella/copo)", "Columnar (Parquet)", "Analistas, BI", "Gold, dashboard, Spark"],
    }), hide_index=True, **W)
    con.close()


# ------------------------------------------- 2. Data Warehouse, Data Lake, Lakehouse
def _inventario(lake):
    filas = []
    for capa in ["bronze", "silver", "gold"]:
        carpeta = lake / capa
        archivos = ([f for f in carpeta.rglob("*") if f.is_file() and f.suffix in (".parquet", ".csv")]
                    if carpeta.exists() else [])
        registros = 0
        for f in archivos:
            try:
                if f.suffix == ".parquet":
                    import pyarrow.parquet as pq
                    registros += pq.ParquetFile(f).metadata.num_rows
                else:
                    registros += max(sum(1 for _ in f.open("rb")) - 1, 0)
            except Exception:
                pass
        filas.append({"capa": capa.capitalize(), "archivos": len(archivos), "registros": registros,
                      "tamaño (KB)": round(sum(f.stat().st_size for f in archivos) / 1024, 1)})
    return pd.DataFrame(filas)


def tema_dw_lake(cat, ctx):
    st.markdown(
        "Un **Data Warehouse** guarda datos limpios y modelados (*schema-on-write*) para BI. Un **Data Lake** "
        "guarda datos crudos de cualquier formato (*schema-on-read*), barato y flexible. Un **Lakehouse** une "
        "ambos: archivos abiertos (Parquet) en el lake con capas, esquema y consultas SQL como un warehouse.")
    st.dataframe(pd.DataFrame({
        "": ["Datos", "Esquema", "Formato", "Costo", "Usuarios", "Calidad/ACID", "Riesgo"],
        "Data Warehouse": ["Estructurados, curados", "Al escribir", "Tablas propietarias", "Alto", "Analistas BI",
                           "Fuerte", "Rígido y caro"],
        "Data Lake": ["Todo: CSV, JSON, logs", "Al leer", "Archivos abiertos", "Bajo", "Ingenieros, ciencia de datos",
                      "Débil", "Se vuelve «pantano»"],
        "Lakehouse": ["Todo, por capas", "Ambos", "Parquet/Delta abiertos", "Bajo-medio", "Todos",
                      "Capas + validación", "Requiere gobierno"],
    }), hide_index=True, **W)

    st.markdown("#### Capas de este proyecto (`data_lake/`)")
    if st.button("⚙️ Generar Bronze → Silver → Gold con este dataset", key="lab_etl_lake"):
        try:
            import pipeline_avanzado as pipeline
            silver, _ = pipeline.ejecutar_etl(_renombrar_pipeline(cat))
            st.success(f"ETL ejecutado: {len(silver):,} filas en Silver.")
        except Exception as exc:
            st.error(f"No se pudo ejecutar el ETL: {exc}")
    inv = _inventario(ctx["lake"])
    st.dataframe(inv, hide_index=True, **W)
    st.caption("**Bronze** = crudo tal como llegó · **Silver** = limpio y tipado · **Gold** = agregado listo para análisis.")

    st.markdown("#### Estilo Lakehouse: SQL sobre los archivos del lake")
    sql = st.text_area("Consulta (solo SELECT). Tablas: `catalogo` y, si existe, `silver`",
                       "SELECT genero, COUNT(*) AS canciones FROM catalogo GROUP BY genero ORDER BY canciones DESC LIMIT 10")
    if st.button("▶ Ejecutar SQL", key="lab_sql"):
        s = sql.strip().rstrip(";")
        if not s.lower().startswith(("select", "with")) or ";" in s:
            st.error("Solo se permite una consulta SELECT.")
            return
        con = sqlite3.connect(":memory:")
        try:
            cat.to_sql("catalogo", con, index=False)
            try:
                import pipeline_avanzado as pipeline
                silver = pipeline._leer_tabla(pipeline.SILVER)
                for c in silver.columns:
                    if "datetime" in str(silver[c].dtype):
                        silver[c] = silver[c].astype(str)
                silver.to_sql("silver", con, index=False)
            except Exception:
                pass
            con.execute("PRAGMA query_only = ON")
            st.dataframe(pd.read_sql_query(s, con), hide_index=True, **W)
        except Exception as exc:
            st.error(f"Error en la consulta: {exc}")
        finally:
            con.close()


# --------------------------------------------------- 3. Modelos dimensionales
def _construir_modelos(cat):
    c = cat.copy()
    c["fecha"] = pd.to_datetime(c["fecha"])
    dim_tiempo = pd.DataFrame({"fecha": np.sort(c["fecha"].dt.normalize().unique())})
    dim_tiempo.insert(0, "fecha_key", np.arange(1, len(dim_tiempo) + 1))
    dim_tiempo["anio"] = dim_tiempo["fecha"].dt.year
    dim_tiempo["mes"] = dim_tiempo["fecha"].dt.month
    c["fecha_d"] = c["fecha"].dt.normalize()
    c = c.merge(dim_tiempo[["fecha", "fecha_key"]].rename(columns={"fecha": "fecha_d"}), on="fecha_d", how="left")
    dim_artista = pd.DataFrame({"artista": c["artista"].unique()})
    dim_artista.insert(0, "artista_key", np.arange(1, len(dim_artista) + 1))
    dim_genero = pd.DataFrame({"genero": c["genero"].unique()})
    dim_genero.insert(0, "genero_key", np.arange(1, len(dim_genero) + 1))
    track_star = c[["id", "titulo", "artista", "genero", "duracion_ms"]].rename(columns={"id": "track_key"})
    track_snow = (c[["id", "titulo", "artista", "genero", "duracion_ms"]].rename(columns={"id": "track_key"})
                  .merge(dim_artista, on="artista").merge(dim_genero, on="genero")
                  [["track_key", "titulo", "duracion_ms", "artista_key", "genero_key"]])
    hecho = c[["id", "fecha_key", "reproducciones"]].rename(columns={"id": "track_key"})
    return dict(hecho=hecho, tiempo=dim_tiempo, artista=dim_artista, genero=dim_genero,
                track_star=track_star, track_snow=track_snow)


_DOT_STYLE = ('rankdir=LR; node [shape=record, style=filled, fillcolor="#1b2027", fontcolor="white", '
              'color="#4b5563"]; edge [color="#9ca3af"];')


def tema_dimensional(cat, ctx):
    st.markdown(
        "Un **modelo dimensional** separa *hechos* (medidas numéricas: reproducciones) de *dimensiones* "
        "(contexto: canción, artista, género, tiempo). En **estrella** las dimensiones están desnormalizadas "
        "(1 salto desde el hecho). En **copo de nieve** se normalizan en sub-dimensiones (menos redundancia, más joins).")
    m = _construir_modelos(cat)
    tab_e, tab_c = st.tabs(["⭐ Estrella", "❄️ Copo de nieve"])
    with tab_e:
        st.graphviz_chart(f'digraph {{ {_DOT_STYLE} hecho [fillcolor="#7c2d12", label="{{hecho_reproducciones|track_key (FK)|fecha_key (FK)|reproducciones}}"]; '
                          'dim_track [label="{dim_track|track_key (PK)|titulo|artista|genero|duracion_ms}"]; '
                          'dim_tiempo [label="{dim_tiempo|fecha_key (PK)|fecha|anio|mes}"]; hecho -> dim_track; hecho -> dim_tiempo; }')
        st.dataframe(m["track_star"].head(8), hide_index=True, **W)
    with tab_c:
        st.graphviz_chart(f'digraph {{ {_DOT_STYLE} hecho [fillcolor="#7c2d12", label="{{hecho_reproducciones|track_key (FK)|fecha_key (FK)|reproducciones}}"]; '
                          'dim_track [label="{dim_track|track_key (PK)|titulo|duracion_ms|artista_key (FK)|genero_key (FK)}"]; '
                          'dim_artista [label="{dim_artista|artista_key (PK)|artista}"]; dim_genero [label="{dim_genero|genero_key (PK)|genero}"]; '
                          'dim_tiempo [label="{dim_tiempo|fecha_key (PK)|fecha|anio|mes}"]; '
                          'hecho -> dim_track; hecho -> dim_tiempo; dim_track -> dim_artista; dim_track -> dim_genero; }')
        st.dataframe(m["track_snow"].head(8), hide_index=True, **W)

    st.markdown("#### Misma pregunta, dos modelos: reproducciones por género y año")
    t0 = _t()
    r_star = (m["hecho"].merge(m["track_star"][["track_key", "genero"]], on="track_key")
              .merge(m["tiempo"][["fecha_key", "anio"]], on="fecha_key")
              .groupby(["genero", "anio"])["reproducciones"].sum())
    t_star = _t() - t0
    t0 = _t()
    r_snow = (m["hecho"].merge(m["track_snow"][["track_key", "genero_key"]], on="track_key")
              .merge(m["genero"], on="genero_key").merge(m["tiempo"][["fecha_key", "anio"]], on="fecha_key")
              .groupby(["genero", "anio"])["reproducciones"].sum())
    t_snow = _t() - t0
    a, b, c = st.columns(3)
    a.metric("⭐ Estrella (2 joins)", f"{t_star * 1000:.1f} ms")
    b.metric("❄️ Copo de nieve (3 joins)", f"{t_snow * 1000:.1f} ms")
    c.metric("Resultados idénticos", "Sí" if r_star.equals(r_snow) else "No")
    top = r_star.groupby("genero").sum().sort_values(ascending=False).head(10)
    st.bar_chart(top)
    st.caption(f"Celdas de dimensión repetidas: estrella guarda artista+género como texto en {len(m['track_star']):,} filas; "
               f"copo de nieve los guarda una vez ({len(m['artista']):,} artistas, {len(m['genero']):,} géneros).")
    if st.button("💾 Guardar estrella y copo de nieve en data_lake/gold", key="lab_dim_save"):
        try:
            import pipeline_avanzado as pipeline
            tablas = pipeline.construir_modelos_dimensionales(_renombrar_pipeline(cat))
            st.success(f"Guardadas {len(tablas)} tablas en data_lake/gold.")
        except Exception as exc:
            st.error(f"No se pudieron guardar: {exc}")


# --------------------------------------------------- 4. Python, NumPy y Pandas
_OPS = [
    ("Filtrar filas", 'cat[cat.duracion_ms > 240_000][["titulo","artista","duracion_ms"]].head(15)',
     lambda c: c[c.duracion_ms > 240_000][["titulo", "artista", "duracion_ms"]].head(15)),
    ("groupby + agg", 'cat.groupby("genero").agg(canciones=("id","count"), duracion_media=("duracion_ms","mean"))',
     lambda c: c.groupby("genero").agg(canciones=("id", "count"), duracion_media=("duracion_ms", "mean"))
     .sort_values("canciones", ascending=False).head(15)),
    ("merge (join)", 'top = cat.groupby("artista").size().rename("n").reset_index()\ncat.merge(top, on="artista").head(15)',
     lambda c: c.merge(c.groupby("artista").size().rename("n").reset_index(), on="artista")
     [["titulo", "artista", "n"]].head(15)),
    ("pivot_table", 'c = cat.assign(anio=cat.fecha.dt.year)\nc.pivot_table(index="genero", columns="anio", values="id", aggfunc="count")',
     lambda c: (lambda x: x[x.genero.isin(x.genero.value_counts().head(8).index)
                            & x.anio.isin(sorted(x.anio.unique())[-6:])]
                .pivot_table(index="genero", columns="anio", values="id", aggfunc="count", fill_value=0))
     (c.assign(anio=c.fecha.dt.year))),
    ("Columna vectorizada", 'cat.assign(duracion_min=np.round(cat.duracion_ms / 60_000, 2))',
     lambda c: c.assign(duracion_min=np.round(c.duracion_ms / 60_000, 2))[["titulo", "duracion_ms", "duracion_min"]].head(15)),
    ("transform (ventana por grupo)", 'cat["vs_media_genero"] = cat.duracion_ms - cat.groupby("genero").duracion_ms.transform("mean")',
     lambda c: c.assign(vs_media_genero=(c.duracion_ms - c.groupby("genero").duracion_ms.transform("mean")).round(0))
     [["titulo", "genero", "duracion_ms", "vs_media_genero"]].head(15)),
]


def tema_numpy_pandas(cat, ctx):
    st.markdown(
        "**NumPy** opera sobre arreglos con código compilado (vectorizado) y **Pandas** construye DataFrames encima. "
        "Evitar bucles de Python es la mayor ganancia de rendimiento en análisis de datos.")
    n = st.select_slider("Filas para la prueba de velocidad", [10_000, 100_000, 500_000, 1_000_000], 100_000)
    datos = np.resize(cat["duracion_ms"].to_numpy(), n)
    lista = datos.tolist()
    tiempos = {}
    t0 = _t(); res = []
    for x in lista:
        res.append(round(x / 60000, 2))
    tiempos["Bucle for"] = _t() - t0
    t0 = _t(); [round(x / 60000, 2) for x in lista]; tiempos["List comprehension"] = _t() - t0
    s = pd.Series(datos)
    t0 = _t(); s.apply(lambda x: round(x / 60000, 2)); tiempos["Pandas apply"] = _t() - t0
    t0 = _t(); np.round(datos / 60000, 2); tiempos["NumPy vectorizado"] = _t() - t0
    ms = pd.Series({k: v * 1000 for k, v in tiempos.items()}, name="milisegundos")
    c1, c2 = st.columns([2, 1])
    c1.bar_chart(ms)
    c2.metric("Speedup NumPy vs bucle", f"{tiempos['Bucle for'] / max(tiempos['NumPy vectorizado'], 1e-9):,.0f}×")
    mins = np.round(datos / 60000, 2)
    a, b, c, d = st.columns(4)
    a.metric("Media (min)", f"{np.mean(mins):.2f}"); b.metric("Mediana", f"{np.median(mins):.2f}")
    c.metric("Desv. estándar", f"{np.std(mins):.2f}"); d.metric("Percentil 90", f"{np.percentile(mins, 90):.2f}")

    st.markdown("#### Operaciones con DataFrames")
    nombre = st.selectbox("Operación", [o[0] for o in _OPS], key="lab_ops")
    _, codigo, fn = next(o for o in _OPS if o[0] == nombre)
    st.code(codigo, language="python")
    st.dataframe(fn(cat), **W)


# ------------------------------------------------------ 5. APIs y extracción
def _extraer_api(termino, limite, reintentos):
    import requests
    intentos = []
    for i in range(reintentos):
        t0 = _t()
        try:
            r = requests.get(ITUNES_URL, params={"term": termino, "entity": "song", "limit": limite}, timeout=(5, 15))
            intentos.append({"intento": i + 1, "resultado": f"HTTP {r.status_code}", "segundos": round(_t() - t0, 2)})
            if r.status_code == 200:
                return {"ok": True, "json": r.json(), "url": r.url, "intentos": intentos, "bytes": len(r.content)}
            if r.status_code not in (429, 500, 502, 503, 504):
                break
        except Exception as exc:
            intentos.append({"intento": i + 1, "resultado": type(exc).__name__, "segundos": round(_t() - t0, 2)})
        if i < reintentos - 1:
            time.sleep(0.5 * 2 ** i)
    return {"ok": False, "intentos": intentos}


def tema_apis(cat, ctx):
    st.markdown(
        "Una **API** expone datos mediante peticiones HTTP: un *endpoint*, *parámetros* y una respuesta (normalmente JSON). "
        "La extracción robusta maneja códigos de estado, límites de tasa, reintentos con espera exponencial y guarda "
        "el crudo en Bronze antes de transformar.")
    c1, c2, c3 = st.columns([2, 1, 1])
    term = c1.text_input("Término", "Coldplay", key="lab_api_term")
    lim = c2.slider("limit", 5, 50, 10, key="lab_api_lim")
    reint = c3.slider("Reintentos", 1, 4, 3, key="lab_api_ret")
    st.code(f"GET {ITUNES_URL}?term={term}&entity=song&limit={lim}", language="http")
    if st.button("▶ Hacer la petición", key="lab_api_go"):
        with st.spinner("Llamando a la API…"):
            st.session_state["lab_api"] = _extraer_api(term, lim, reint)
    res = st.session_state.get("lab_api")
    if not res:
        return
    st.dataframe(pd.DataFrame(res["intentos"]), hide_index=True, **W)
    if not res["ok"]:
        st.error("No se obtuvo respuesta válida (¿sin internet o API limitando?). Los reintentos usan espera 0.5·2ⁿ s.")
        return
    js = res["json"]
    a, b = st.columns(2)
    a.metric("Resultados", js.get("resultCount", 0)); b.metric("Tamaño respuesta", f"{res['bytes'] / 1024:.1f} KB")
    if js.get("results"):
        st.markdown("**JSON crudo (primer resultado)**")
        st.json(js["results"][0], expanded=False)
        plano = pd.json_normalize(js["results"])
        st.markdown(f"**Aplanado a DataFrame** ({plano.shape[0]} filas × {plano.shape[1]} columnas)")
        st.dataframe(plano.head(10), **W)
        if st.button("💾 Guardar como capa Bronze", key="lab_api_save"):
            try:
                import pipeline_avanzado as pipeline
                dest = pipeline._guardar_tabla(plano, pipeline.BRONZE / f"itunes_api_{datetime.now():%Y%m%d_%H%M%S}")
                st.success(f"Guardado en {dest}")
            except Exception as exc:
                st.error(f"No se pudo guardar: {exc}")


# ------------------------------------------------- 6. Spark y procesamiento distribuido
@st.cache_resource(show_spinner=False)
def _spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName("LabDashboard").master("local[2]")
            .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate())


def tema_spark(cat, ctx):
    st.markdown(
        "**Apache Spark** reparte los datos en *particiones* que procesan en paralelo los *executors*, coordinados por un "
        "*driver*. Cada partición hace un trabajo parcial (*map*) y luego se combinan resultados (*shuffle/reduce*). "
        "Si el reparto es desigual (*skew*), la partición más grande marca el tiempo total.")
    c1, c2 = st.columns(2)
    n_part = c1.slider("Particiones (≈ núcleos/executors)", 1, 8, 4, key="lab_spark_p")
    modo = c2.radio("Reparto de datos", ["Round-robin (parejo)", "Hash por artista (puede haber skew)"], key="lab_spark_m")
    if modo.startswith("Round"):
        part = np.arange(len(cat)) % n_part
    else:
        part = pd.util.hash_pandas_object(cat["artista"], index=False).to_numpy() % n_part
    cat_p = cat.assign(particion=part)
    parciales = cat_p.groupby(["particion", "genero"]).size().rename("n").reset_index()
    tam = cat_p.groupby("particion").size().rename("filas")
    final = parciales.groupby("genero")["n"].sum().sort_values(ascending=False)
    ok = final.equals(cat.groupby("genero").size().sort_values(ascending=False).rename("n"))
    a, b = st.columns(2)
    with a:
        st.markdown("**Filas por partición** (la más grande es la tarea más lenta)")
        st.bar_chart(tam)
        st.caption(f"Desbalance: máx/mín = {tam.max() / max(tam.min(), 1):.2f}×")
    with b:
        st.markdown("**Resultado combinado (reduce)**")
        st.dataframe(final.head(10).rename("filas"), **W)
        st.caption("Coincide con el cálculo global de Pandas." if ok else "⚠️ No coincide con el cálculo global.")
    with st.expander("Ver el trabajo parcial de cada partición (map)"):
        st.dataframe(parciales, hide_index=True, **W)

    st.markdown("#### Spark real (modo local)")
    st.caption("Requiere `pyspark` y Java. La primera vez tarda en arrancar la sesión.")
    if st.button("▶ Ejecutar groupBy con Spark", key="lab_spark_go"):
        try:
            with st.spinner("Iniciando Spark…"):
                spark = _spark()
                t0 = _t()
                filas = list(zip(cat["genero"].astype(str), cat["artista"].astype(str), cat["duracion_ms"].astype(int).tolist()))
                sdf = spark.createDataFrame(filas, "genero string, artista string, duracion_ms long").repartition(n_part)
                from pyspark.sql import functions as F
                agg = sdf.groupBy("genero").agg(F.count("*").alias("canciones"), F.avg("duracion_ms").alias("dur_media"))
                filas_out = agg.orderBy(F.desc("canciones")).collect()
                tam_spark = sdf.rdd.glom().map(len).collect()
                try:
                    plan = agg._jdf.queryExecution().simpleString()
                except Exception:
                    plan = "(plan no disponible en este modo de Spark)"
            st.session_state["lab_spark"] = {
                "tabla": pd.DataFrame([r.asDict() for r in filas_out]), "tam": tam_spark,
                "seg": _t() - t0, "plan": plan, "ver": spark.version}
        except Exception as exc:
            st.session_state["lab_spark"] = {"error": str(exc)}
    r = st.session_state.get("lab_spark")
    if r:
        if "error" in r:
            st.error(f"Spark no pudo ejecutarse: {r['error'][:400]}")
        else:
            st.success(f"Spark {r['ver']} · {r['seg']:.1f} s · particiones reales: {r['tam']}")
            st.dataframe(r["tabla"].head(10), hide_index=True, **W)
            st.markdown("**Plan físico**")
            st.code(r["plan"], language=None)


# ------------------------- 7. Transformaciones, acciones, batch y streaming
def tema_lazy_batch(cat, ctx):
    st.markdown(
        "En Spark las **transformaciones** (`filter`, `select`, `groupBy`…) son *perezosas*: solo construyen un plan. "
        "Nada se calcula hasta una **acción** (`count`, `collect`, `show`, `write`). Las transformaciones *estrechas* "
        "operan por partición; las *anchas* (`groupBy`, `dropDuplicates`) requieren *shuffle*.")
    min_dur = st.slider("Duración mínima (min) para el filtro", 1.0, 6.0, 3.0, 0.5)
    pasos_def = {
        "filter · duración > mínimo": ("estrecha", lambda d: d[d.duracion_ms > min_dur * 60000]),
        "withColumn · duracion_min": ("estrecha", lambda d: d.assign(duracion_min=(d.duracion_ms / 60000).round(2))),
        "dropDuplicates · (titulo, artista)": ("ancha", lambda d: d.drop_duplicates(["titulo", "artista"])),
        "groupBy · genero → count": ("ancha", lambda d: d.groupby("genero").size().rename("canciones").reset_index()),
    }
    sel = st.multiselect("Transformaciones del plan (se aplican en este orden)", list(pasos_def),
                         default=list(pasos_def)[:2], key="lab_lazy_steps")
    plan = [s for s in pasos_def if s in sel]
    st.markdown("**Plan lógico acumulado**")
    if plan:
        st.code("\n".join(f"{i}. {s}   [{pasos_def[s][0]}]" for i, s in enumerate(plan, 1)) +
                "\n\n→ Filas procesadas hasta ahora: 0  (nada se ejecuta sin una acción)", language=None)
    else:
        st.info("Elige al menos una transformación.")

    def ejecutar():
        d, log = cat, []
        for s in plan:
            t0 = _t(); n_in = len(d); d = pasos_def[s][1](d)
            log.append({"paso": s, "tipo": pasos_def[s][0], "filas_entrada": n_in, "filas_salida": len(d),
                        "ms": round((_t() - t0) * 1000, 2)})
        return d, pd.DataFrame(log)

    a, b, c = st.columns(3)
    p1, p2, p3 = a.button("▶ count()", key="lab_a1"), b.button("▶ take(5)", key="lab_a2"), c.button("▶ collect()", key="lab_a3")
    accion = "count()" if p1 else "take(5)" if p2 else "collect()" if p3 else None
    if accion and plan:
        d, log = ejecutar()
        st.markdown(f"**Acción `{accion}` disparó la ejecución**")
        st.dataframe(log, hide_index=True, **W)
        if accion == "count()":
            st.metric("Resultado", f"{len(d):,}")
        else:
            st.dataframe(d.head(5) if accion == "take(5)" else d.head(200), **W)

    st.markdown("#### Procesamiento batch vs streaming")
    ev = cat.sample(min(600, len(cat)), random_state=3).reset_index(drop=True)
    tam = st.slider("Tamaño del micro-lote (streaming)", 20, 200, 60, 20, key="lab_lazy_mb")
    ev["lote"] = np.arange(len(ev)) // tam
    acum = pd.crosstab(ev["lote"], ev["genero"]).cumsum()
    top = acum.iloc[-1].sort_values(ascending=False).head(4).index
    a, b = st.columns(2)
    a.metric("Batch: primer resultado", f"tras leer {len(ev)} filas")
    b.metric("Streaming: primer resultado", f"tras {tam} filas", f"se actualiza cada {tam}", delta_color="off")
    st.line_chart(acum[top])
    st.caption("Cada punto es el resultado acumulado tras un micro-lote. Batch entrega solo el último punto, al final.")


# ------------------------------------------------------ 14. ETL, ELT y pipelines
def _ensuciar(cat):
    rng = np.random.default_rng(5)
    raw = cat.copy()
    n = len(raw)
    ix = rng.choice(n, max(1, n // 20), replace=False)
    raw.loc[ix, "artista"] = "  " + raw.loc[ix, "artista"].str.upper() + " "
    nul = rng.choice(n, max(1, n // 50), replace=False)
    raw.loc[nul, "titulo"] = None
    dup = raw.sample(max(1, n // 10), random_state=2)
    return pd.concat([raw, dup], ignore_index=True)


def tema_etl_elt(cat, ctx):
    st.markdown(
        "**ETL**: Extraer → *Transformar* → Cargar: se limpia antes de guardar (típico del warehouse tradicional). "
        "**ELT**: Extraer → Cargar el crudo → *Transformar dentro* del destino con su motor (típico del lake/lakehouse). "
        "Un **pipeline** encadena pasos con validaciones y registro de cada ejecución.")
    raw = _ensuciar(cat)
    st.caption(f"Datos de entrada con suciedad inyectada: {len(raw):,} filas (duplicados, espacios/mayúsculas, títulos nulos).")

    t0 = _t()
    etl = raw.dropna(subset=["titulo"]).copy()
    etl["titulo_n"] = etl["titulo"].str.strip().str.lower()
    etl["artista_n"] = etl["artista"].str.strip().str.lower()
    etl = etl.drop_duplicates(["titulo_n", "artista_n"])
    con1 = sqlite3.connect(":memory:")
    etl[["id", "titulo_n", "artista_n", "genero", "duracion_ms"]].to_sql("clean", con1, index=False)
    t_etl = _t() - t0
    n_etl = con1.execute("SELECT COUNT(*) FROM clean").fetchone()[0]
    con1.close()

    t0 = _t()
    con2 = sqlite3.connect(":memory:")
    raw[["id", "titulo", "artista", "genero", "duracion_ms"]].to_sql("raw", con2, index=False)
    con2.execute("""CREATE TABLE clean AS SELECT MIN(id) AS id, LOWER(TRIM(titulo)) AS titulo_n,
                    LOWER(TRIM(artista)) AS artista_n, MIN(genero) AS genero, MIN(duracion_ms) AS duracion_ms
                    FROM raw WHERE titulo IS NOT NULL GROUP BY LOWER(TRIM(titulo)), LOWER(TRIM(artista))""")
    t_elt = _t() - t0
    n_elt = con2.execute("SELECT COUNT(*) FROM clean").fetchone()[0]
    n_raw = con2.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    con2.close()

    a, b = st.columns(2)
    with a:
        st.markdown("#### ETL (transforma en Pandas, carga limpio)")
        st.metric("Filas cargadas", f"{n_etl:,}", f"{t_etl * 1000:.0f} ms", delta_color="off")
        st.caption("El destino solo ve datos limpios; el crudo no se conserva.")
    with b:
        st.markdown("#### ELT (carga crudo, transforma con SQL)")
        st.metric("Filas en tabla limpia", f"{n_elt:,}", f"{t_elt * 1000:.0f} ms", delta_color="off")
        st.caption(f"El crudo ({n_raw:,} filas) queda disponible en el destino para reprocesar.")
    st.success("Ambos caminos producen el mismo resultado.") if n_etl == n_elt else st.warning("Los conteos difieren.")

    st.markdown("#### Pipeline con validaciones")
    gate = st.slider("Máximo % de títulos nulos permitido (quality gate)", 0.0, 10.0, 5.0, 0.5)
    log, d = [], raw
    pct_nulos = raw["titulo"].isna().mean() * 100

    def paso(nombre, fn, ok=True, detalle=""):
        nonlocal d
        t0 = _t(); n_in = len(d)
        if ok:
            d = fn(d)
        log.append({"paso": nombre, "filas_entrada": n_in, "filas_salida": len(d) if ok else n_in,
                    "ms": round((_t() - t0) * 1000, 2), "estado": ("✅ OK" if ok else "❌ detenido") + (f" {detalle}" if detalle else "")})
        return ok

    paso("1. Extract", lambda x: x)
    continuar = pct_nulos <= gate
    paso("2. Validate (quality gate)", lambda x: x, continuar, f"nulos {pct_nulos:.1f}% vs máx {gate}%")
    if continuar:
        paso("3. Clean", lambda x: x.dropna(subset=["titulo"]).assign(artista=lambda y: y.artista.str.strip().str.title()))
        paso("4. Deduplicate", lambda x: x.drop_duplicates(["titulo", "artista"]))
        paso("5. Enrich", lambda x: x.assign(duracion_min=(x.duracion_ms / 60000).round(2)))
        paso("6. Load (Gold por género)", lambda x: x.groupby("genero").agg(canciones=("id", "count"), dur_media=("duracion_min", "mean")).reset_index())
    st.dataframe(pd.DataFrame(log), hide_index=True, **W)
    if continuar:
        st.dataframe(d.head(10), hide_index=True, **W)
        if st.button("💾 Persistir resultado en data_lake (Bronze/Silver/Gold)", key="lab_etl_save"):
            try:
                import pipeline_avanzado as pipeline
                silver, _ = pipeline.ejecutar_etl(_renombrar_pipeline(raw.drop(columns=["fecha"], errors="ignore")))
                st.success(f"Persistido: {len(silver):,} filas en Silver.")
            except Exception as exc:
                st.error(f"No se pudo persistir: {exc}")
    else:
        st.error("El pipeline se detuvo en la validación: los datos no cumplen el umbral de calidad.")
