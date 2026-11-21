"""Laboratorio de temas de clase — parte 2 (streaming).

Kafka y Spark Structured Streaming se SIMULAN en Python para que todo corra sin broker ni Java
(la parte Kafka reutiliza las clases Topico/Productor/Consumidor de streaming.py).
Donde existe una versión real (Spark) hay un botón aparte. Flink y Kafka Streams se explican
y comparan, pero no se ejecutan.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import streamlit as st

from lab_batch import W

T0 = datetime(2026, 11, 20, 10, 0, 0)


def _hora(seg):
    return (T0 + timedelta(seconds=float(seg))).strftime("%H:%M:%S")


def _eventos(cat, n, semilla=42):
    m = cat.sample(min(n, len(cat)), random_state=semilla).reset_index(drop=True)
    return [{"evento_id": f"EV-{i + 1:04d}", "artista": r.artista, "cancion": r.titulo,
             "genero": r.genero, "reproducciones": int(r.reproducciones)} for i, r in enumerate(m.itertuples())]


# ---------------------------------------------------------------- 8. Kafka
def tema_kafka(cat, ctx):
    import streaming as stm
    st.markdown(
        "**Kafka** es un log distribuido de eventos. Un **topic** se divide en **particiones** (cada evento recibe un "
        "*offset*). Los **brokers** son los servidores que guardan las particiones (un *líder* por partición y réplicas en "
        "otros brokers). Los **productores** publican; los **consumidores** leen en **grupos**: dentro de un grupo cada "
        "partición la lee un solo consumidor; otro grupo lee todo por separado.")
    c1, c2, c3, c4 = st.columns(4)
    n_part = c1.slider("Particiones", 1, 6, 3, key="k_part")
    n_brk = c2.slider("Brokers", 1, 5, 3, key="k_brk")
    repl = c3.slider("Replicación", 1, min(3, n_brk), min(2, n_brk), key="k_rep") if n_brk > 1 else 1
    n_ev = c4.slider("Eventos", 10, 200, 60, 10, key="k_ev")
    con_clave = st.radio("Clave del mensaje", ["Clave = artista (mismo artista → misma partición)", "Sin clave (round-robin)"],
                         horizontal=True, key="k_key").startswith("Clave")

    topico = stm.Topico("spotify.reproducciones", n_part)
    prod = stm.Productor(topico)
    filas = []
    for e in _eventos(cat, n_ev):
        p, off = prod.enviar(e, clave=e["artista"] if con_clave else None)
        filas.append({"partición": p, "offset": off, "artista": e["artista"], "canción": e["cancion"]})
    reg = pd.DataFrame(filas)
    tam = reg.groupby("partición").size().reindex(range(n_part), fill_value=0)

    a, b = st.columns(2)
    with a:
        st.markdown("**Eventos por partición**")
        st.bar_chart(tam)
        if con_clave:
            st.caption("Con clave el reparto puede ser desigual, pero se conserva el orden por artista.")
    with b:
        st.markdown("**Registro del topic (partición, offset)**")
        st.dataframe(reg.sort_values(["partición", "offset"]).head(15), hide_index=True, **W)

    st.markdown("#### Brokers, líderes y réplicas")
    caido = st.selectbox("Simular caída de un broker", ["Ninguno"] + [f"Broker {i}" for i in range(n_brk)], key="k_down")
    down = None if caido == "Ninguno" else int(caido.split()[1])
    estado = []
    for p in range(n_part):
        reps = [(p + i) % n_brk for i in range(repl)]
        lider = reps[0]
        if down is not None and lider == down:
            vivos = [r for r in reps if r != down]
            nuevo = vivos[0] if vivos else None
            est = f"⚠️ failover → Broker {nuevo}" if vivos else "❌ no disponible"
        else:
            nuevo, est = lider, "✅ normal"
        estado.append({"partición": p, "réplicas (brokers)": str(reps), "líder": f"Broker {lider}",
                       "líder actual": "—" if nuevo is None else f"Broker {nuevo}", "estado": est})
    st.dataframe(pd.DataFrame(estado), hide_index=True, **W)
    if repl == 1 and down is not None:
        st.warning("Con replicación 1 la caída de un broker deja sus particiones sin servicio. Por eso se replica.")

    st.markdown("#### Grupos de consumidores")
    n_cons = st.slider("Consumidores en el grupo «analitica»", 1, 6, 2, key="k_cons")
    asign = {f"Consumidor {i}": [] for i in range(n_cons)}
    for p in range(n_part):
        asign[f"Consumidor {p % n_cons}"].append(p)
    st.dataframe(pd.DataFrame([{"consumidor": k, "particiones": str(v) if v else "— (inactivo)",
                                "eventos": int(sum(tam[p] for p in v))} for k, v in asign.items()]),
                 hide_index=True, **W)
    if n_cons > n_part:
        st.caption("Hay más consumidores que particiones: los sobrantes quedan inactivos.")
    pct = st.slider("Avance del grupo «analitica» (% leído)", 0, 100, 50, 10, key="k_pct")
    ga = stm.Consumidor(topico, "analitica")
    gb = stm.Consumidor(topico, "auditoria")
    ga.leer(maximo=int(topico.total_eventos() * pct / 100))
    off = topico.offsets("analitica")
    lag = [len(topico.particiones[p]) - off[p] for p in range(n_part)]
    st.dataframe(pd.DataFrame({"partición": range(n_part), "total": [len(x) for x in topico.particiones],
                               "offset del grupo": off, "lag (pendientes)": lag}), hide_index=True, **W)
    leidos_b = len(gb.leer())
    st.metric("Grupo «auditoria» (independiente)", f"{leidos_b} eventos leídos",
              "lee todo desde el inicio sin afectar al otro grupo", delta_color="off")


# ------------------------------------------------ 9. Spark Structured Streaming
def tema_structured(cat, ctx):
    st.markdown(
        "**Structured Streaming** trata un flujo como una *tabla sin límite* a la que se le agregan filas. Spark procesa "
        "**micro-lotes** (trigger) y escribe según el **modo de salida**: `append` (solo filas nuevas finales), "
        "`update` (solo lo que cambió) y `complete` (toda la tabla resultado).")
    ev = pd.DataFrame(_eventos(cat, 60, semilla=9))
    tam = st.slider("Eventos por micro-lote", 5, 20, 10, key="ss_tam")
    n_lotes = math.ceil(len(ev) / tam)
    k = st.slider("Micro-lote actual", 1, n_lotes, min(3, n_lotes), key="ss_k")
    consulta = st.radio("Consulta", ["Filtro sin estado (reproducciones altas)", "Agregación con estado (conteo por género)"],
                        horizontal=True, key="ss_q")
    modo = st.radio("Modo de salida", ["append", "update", "complete"], horizontal=True, key="ss_mode")
    umbral = float(ev["reproducciones"].quantile(0.5))
    lote = ev.iloc[(k - 1) * tam: k * tam]
    hasta = ev.iloc[: k * tam]
    a, b = st.columns(2)
    with a:
        st.markdown(f"**Tabla de entrada** (llegaron {len(hasta)} eventos)")
        st.dataframe(lote[["evento_id", "genero", "reproducciones"]], hide_index=True, **W)
        st.caption(f"Filas nuevas del micro-lote {k}")
    with b:
        st.markdown("**Salida del micro-lote**")
        if consulta.startswith("Filtro"):
            if modo == "complete":
                st.error("`complete` solo está soportado con agregaciones, no con una consulta sin estado.")
            else:
                st.dataframe(lote[lote["reproducciones"] > umbral][["evento_id", "genero", "reproducciones"]],
                             hide_index=True, **W)
                st.caption(f"Eventos con reproducciones > {umbral:,.0f}. En consultas sin estado, append y update coinciden.")
        else:
            total = hasta.groupby("genero").size().rename("conteo")
            if modo == "append":
                st.error("`append` no se permite en una agregación sin watermark: el conteo de un género puede seguir "
                         "cambiando, así que Spark no puede emitirlo como «final». Usa update/complete o añade watermark + ventana.")
            elif modo == "update":
                st.dataframe(total[total.index.isin(lote["genero"].unique())], **W)
                st.caption("Solo los géneros que cambiaron en este micro-lote.")
            else:
                st.dataframe(total.sort_values(ascending=False), **W)
                st.caption("Toda la tabla resultado, reescrita en cada micro-lote.")
    st.markdown("#### Spark real")
    st.caption("Ejecuta `pipeline_avanzado.spark_streaming_ventanas()`: lee JSON, ventanas de 5 min, watermark de 10 min, "
               "checkpoint y salida Parquet. Requiere pyspark y Java.")
    if st.button("▶ Ejecutar Structured Streaming real", key="ss_real"):
        try:
            import pipeline_avanzado as pipeline
            with st.spinner("Spark procesando (trigger once)…"):
                pipeline.spark_streaming_ventanas()
                out = pipeline._leer_tabla(pipeline.DATA / "stream_output" / "ventanas_artista")
            out = out.copy()
            out["window"] = out["window"].astype(str)
            st.session_state["ss_out"] = out
        except Exception as exc:
            st.session_state["ss_out"] = str(exc)
    r = st.session_state.get("ss_out")
    if isinstance(r, str):
        st.error(f"No se pudo ejecutar: {r[:400]}")
    elif r is not None:
        st.success("Consulta terminada. Ventanas escritas en data_lake/stream_output y checkpoint en data_lake/checkpoints.")
        st.dataframe(r, hide_index=True, **W)


# ----------------------------------- simulación de tiempo de evento y watermark
def _simular(cat, n, retraso, tardios, semilla=4):
    rng = np.random.default_rng(semilla)
    et = np.sort(rng.uniform(0, 20 * 60, n))
    delay = rng.exponential(retraso, n)
    if tardios:
        delay[rng.choice(n, min(tardios, n), replace=False)] = rng.uniform(8 * 60, 15 * 60, min(tardios, n))
    art = cat["artista"].value_counts().head(5).index.to_numpy()
    ev = pd.DataFrame({"evento_id": [f"E{i + 1:03d}" for i in range(n)], "artista": rng.choice(art, n),
                       "event_s": et.round(1), "arrival_s": (et + delay).round(1)})
    return ev.sort_values("arrival_s").reset_index(drop=True)


def _inicios(t, size, slide):
    k, out = int(t // slide), []
    while k >= 0 and k * slide + size > t:
        out.append(k * slide)
        k -= 1
    return out


def _procesar(ev, size, slide, wm_s):
    max_et, estados, wms = -1.0, [], []
    cnt_wm, cnt_ideal, cnt_proc, cnt_tarde = {}, {}, {}, {}
    for r in ev.itertuples():
        fuera_de_orden = r.event_s < max_et
        max_et = max(max_et, r.event_s)
        wm = max_et - wm_s
        wms.append(wm)
        starts = _inicios(r.event_s, size, slide)
        abiertas = [s for s in starts if s + size > wm]
        for s in starts:
            cnt_ideal[s] = cnt_ideal.get(s, 0) + 1
        for s in _inicios(r.arrival_s, size, slide):
            cnt_proc[s] = cnt_proc.get(s, 0) + 1
        if not abiertas:
            estados.append("❌ descartado (tardío)")
            continue
        for s in abiertas:
            cnt_wm[s] = cnt_wm.get(s, 0) + 1
            if fuera_de_orden:
                cnt_tarde[s] = cnt_tarde.get(s, 0) + 1
        estados.append("⚠️ tardío pero aceptado" if fuera_de_orden else "✅ en orden")
    ev = ev.assign(watermark_s=np.round(wms, 1), estado=estados)
    return ev, cnt_wm, cnt_ideal, cnt_proc, cnt_tarde, max_et - wm_s


def _tabla_ventanas(size, **series):
    claves = sorted(set().union(*[set(s) for s in series.values()]))
    return pd.DataFrame({"ventana": [f"{_hora(k)}–{_hora(k + size)}" for k in claves],
                         **{n: [s.get(k, 0) for k in claves] for n, s in series.items()}}).set_index("ventana")


def tema_tiempo_watermark(cat, ctx):
    st.markdown(
        "**Event time** es cuándo ocurrió el evento (viene en el dato). **Processing time** es cuándo lo procesa el "
        "sistema. Con redes lentas llegan fuera de orden. El **watermark** = (mayor event time visto) − umbral: indica hasta "
        "dónde se considera que ya llegó casi todo; las ventanas anteriores se cierran y los eventos muy tardíos se descartan.")
    c1, c2, c3, c4 = st.columns(4)
    n = c1.slider("Eventos", 60, 300, 150, 30, key="tw_n")
    ret = c2.slider("Retraso medio (s)", 5, 60, 20, key="tw_r")
    tard = c3.slider("Eventos muy tardíos", 0, 15, 6, key="tw_t")
    wm_min = c4.slider("Umbral watermark (min)", 0, 15, 5, key="tw_w")
    size_min = st.slider("Ventana tumbling (min)", 1, 10, 5, key="tw_size")
    ev = _simular(cat, n, ret, tard)
    size = size_min * 60
    ev, c_wm, c_ideal, c_proc, _, _ = _procesar(ev, size, size, wm_min * 60)
    desc = int((ev["estado"].str.startswith("❌")).sum())
    a, b, c = st.columns(3)
    a.metric("Eventos", len(ev)); b.metric("Descartados por watermark", desc)
    c.metric("Fuera de orden aceptados", int(ev["estado"].str.startswith("⚠️").sum()))
    tabla = _tabla_ventanas(size, **{"Event time (ideal)": c_ideal, "Event time + watermark": c_wm,
                                     "Processing time": c_proc})
    st.markdown("**Eventos por ventana según el reloj usado**")
    st.bar_chart(tabla)
    st.caption("Processing time reparte los eventos por cuándo llegaron, no por cuándo ocurrieron: distorsiona los resultados. "
               "Con watermark, los eventos más tardíos que el umbral se pierden (la columna baja respecto al ideal).")
    st.markdown("**Línea de tiempo de llegada** (el watermark avanza con el mayor event time visto)")
    ver = ev.assign(event_time=ev.event_s.map(_hora), processing_time=ev.arrival_s.map(_hora),
                    watermark=ev.watermark_s.map(lambda s: _hora(s) if s > 0 else "—"))
    solo = st.checkbox("Mostrar solo eventos tardíos/descartados", True, key="tw_only")
    ver = ver[ver.estado != "✅ en orden"] if solo else ver
    st.dataframe(ver[["evento_id", "artista", "event_time", "processing_time", "watermark", "estado"]].head(60),
                 hide_index=True, **W)


def tema_ventanas(cat, ctx):
    st.markdown(
        "Las **ventanas** agrupan eventos por tiempo. *Tumbling*: fijas y sin solaparse. *Sliding*: se solapan (un evento cae "
        "en varias). *Session*: se cierran tras un periodo de inactividad. Los **eventos tardíos** pueden actualizar una "
        "ventana ya calculada, mientras el watermark no la haya cerrado.")
    tipo = st.radio("Tipo de ventana", ["Tumbling", "Sliding", "Session"], horizontal=True, key="vt_tipo")
    c1, c2, c3 = st.columns(3)
    size_min = c1.slider("Tamaño (min)", 1, 10, 5, key="vt_size")
    slide_min = c2.slider("Salto/slide (min)", 1, size_min, max(1, size_min // 2), key="vt_slide") if tipo == "Sliding" and size_min > 1 else size_min
    wm_min = c3.slider("Umbral watermark (min)", 0, 15, 4, key="vt_wm")
    ev = _simular(cat, 150, 20, 8, semilla=8)
    if tipo == "Session":
        gap = st.slider("Inactividad que cierra la sesión (s)", 5, 120, 30, key="vt_gap")
        filas = []
        for art, g in ev.sort_values("event_s").groupby("artista"):
            t = g["event_s"].to_numpy()
            ini = 0
            for i in range(1, len(t) + 1):
                if i == len(t) or t[i] - t[i - 1] > gap:
                    filas.append({"artista": art, "inicio": _hora(t[ini]), "fin": _hora(t[i - 1]),
                                  "eventos": i - ini, "duración (s)": round(t[i - 1] - t[ini], 1)})
                    ini = i
        sesiones = pd.DataFrame(filas).sort_values("eventos", ascending=False)
        a, b = st.columns(2)
        a.metric("Sesiones", len(sesiones)); b.metric("Eventos por sesión (media)", f"{sesiones.eventos.mean():.1f}")
        st.dataframe(sesiones.head(25), hide_index=True, **W)
        st.caption("Las sesiones no tienen tamaño fijo: dependen del patrón de actividad de cada artista.")
        return
    size, slide = size_min * 60, slide_min * 60
    ev, c_wm, c_ideal, _, c_tarde, wm_final = _procesar(ev, size, slide, wm_min * 60)
    cerradas = {s for s in c_ideal if s + size <= wm_final}
    tabla = _tabla_ventanas(size, **{"Eventos (ideal)": c_ideal, "Eventos (con watermark)": c_wm,
                                     "Actualizada por tardíos": c_tarde})
    tabla["estado"] = ["🔒 cerrada" if k in cerradas else "🔓 abierta" for k in sorted(c_ideal)]
    a, b, c = st.columns(3)
    a.metric("Ventanas", len(tabla)); b.metric("Descartados", int(ev.estado.str.startswith("❌").sum()))
    c.metric("Ventanas con tardíos aceptados", int((tabla["Actualizada por tardíos"] > 0).sum()))
    st.dataframe(tabla, **W)
    st.bar_chart(tabla[["Eventos (ideal)", "Eventos (con watermark)"]])
    if tipo == "Sliding":
        st.caption(f"Con ventana de {size_min} min y salto de {slide_min} min cada evento cuenta en hasta "
                   f"{math.ceil(size / slide)} ventanas.")


# ------------------------------------------------ 12. Tolerancia a fallos
def tema_tolerancia(cat, ctx):
    st.markdown(
        "Un job de streaming de larga duración debe sobrevivir a caídas. Spark guarda en un **checkpoint** los *offsets* "
        "leídos y el *estado* de las agregaciones; Kafka replica las particiones entre brokers. Al reiniciar, la consulta "
        "retoma exactamente donde quedó. Sin checkpoint se pierde o se duplica información.")
    ev = pd.DataFrame(_eventos(cat, 120, semilla=3))
    lotes = [ev.iloc[i:i + 10] for i in range(0, 120, 10)]
    k = st.slider("El proceso se cae mientras procesaba el micro-lote…", 2, 11, 6, key="tf_k")

    def conteo(ls):
        return pd.concat(ls).groupby("genero").size() if ls else pd.Series(dtype=int)

    ref = conteo(lotes)
    carpeta = ctx["lake"] / "checkpoints" / "demo_dashboard"
    carpeta.mkdir(parents=True, exist_ok=True)
    estado = {}
    for i, lt in enumerate(lotes[:k - 1], start=1):          # lotes confirmados antes de la caída
        for g, n in lt.groupby("genero").size().items():
            estado[g] = estado.get(g, 0) + int(n)
        (carpeta / "commit.json").write_text(json.dumps({"ultimo_lote_confirmado": i, "estado": estado,
                                                           "offset_kafka": i * 10}), encoding="utf-8")
    cp = json.loads((carpeta / "commit.json").read_text(encoding="utf-8"))
    a_ = conteo(lotes[k:])                                    # A: reinicia en «latest», sin estado
    b_ = conteo(lotes[:k - 1]).add(ref, fill_value=0)         # B: reprocesa desde el inicio, salida ya escrita
    c_ = pd.Series(cp["estado"]).add(conteo(lotes[k - 1:]), fill_value=0)   # C: checkpoint
    tabla = pd.DataFrame({"Sin fallo (referencia)": ref, "A · sin checkpoint, desde «latest»": a_,
                          "B · sin checkpoint, desde el inicio": b_, "C · con checkpoint": c_}).fillna(0).astype(int)
    tot = tabla.sum()
    c1, c2, c3 = st.columns(3)
    c1.metric("A · pierde datos", int(tot.iloc[1] - tot.iloc[0]), "vs referencia", delta_color="inverse")
    c2.metric("B · duplica datos", int(tot.iloc[2] - tot.iloc[0]), "vs referencia", delta_color="inverse")
    c3.metric("C · con checkpoint", int(tot.iloc[3] - tot.iloc[0]), "vs referencia", delta_color="off")
    st.dataframe(tabla, **W)
    st.success("✅ Con checkpoint el resultado es idéntico a la ejecución sin fallo.") if tabla.iloc[:, 3].equals(tabla.iloc[:, 0]) \
        else st.error("El resultado con checkpoint difiere de la referencia.")
    st.markdown("**Contenido del checkpoint** (escrito en `data_lake/checkpoints/demo_dashboard/commit.json`)")
    st.json(cp, expanded=False)
    st.caption("Semánticas de entrega: *at-most-once* (puede perder), *at-least-once* (puede duplicar), *exactly-once* "
               "(checkpoint + escritura idempotente, p. ej. deduplicar por evento_id o batch_id).")


# ------------------------------------------------ 13. Flink y Kafka Streams
def tema_flink(cat, ctx):
    st.info("Este tema es **conceptual y comparativo**: aquí no se ejecuta un clúster Flink ni una app Kafka Streams real; "
            "la simulación de latencia es un modelo simplificado.")
    st.markdown(
        "**Apache Flink** procesa evento por evento con estado gestionado y event time nativo (streaming-first). "
        "**Kafka Streams** es una librería Java/Scala que corre dentro de tu aplicación y lee/escribe solo en Kafka. "
        "**Spark Structured Streaming** procesa en micro-lotes y unifica batch y streaming con la misma API.")
    st.dataframe(pd.DataFrame({
        "": ["Modelo", "Latencia típica", "Despliegue", "Lenguajes", "Estado", "Event time / watermarks",
             "Fuentes", "Ideal para"],
        "Spark Structured Streaming": ["Micro-lotes", "Segundos", "Clúster Spark", "Python, SQL, Scala, Java",
                                       "Sí (checkpoint)", "Sí", "Kafka, archivos, etc.", "Batch + streaming unificados, analítica"],
        "Apache Flink": ["Evento a evento", "Milisegundos", "Clúster Flink", "Java, Scala, Python, SQL",
                         "Sí, muy robusto", "Sí, muy completo", "Kafka, archivos, etc.", "Baja latencia, estado grande, CEP"],
        "Kafka Streams": ["Evento a evento", "Milisegundos", "Dentro de tu app (sin clúster)", "Java, Scala",
                          "Sí (stores locales)", "Básico", "Solo Kafka", "Microservicios sobre Kafka"],
    }), hide_index=True, **W)

    st.markdown("#### Latencia: micro-lote vs evento a evento (modelo simplificado)")
    c1, c2 = st.columns(2)
    iv = c1.slider("Intervalo del micro-lote (s)", 0.5, 10.0, 2.0, 0.5, key="fl_iv")
    proc = c2.slider("Procesamiento por evento (ms)", 1, 200, 20, key="fl_proc")
    t = np.random.default_rng(1).uniform(0, 60, 500)
    lat_micro = (np.ceil(t / iv) * iv - t) + proc / 1000
    lat_evento = np.full_like(t, proc / 1000)
    a, b = st.columns(2)
    a.metric("Micro-lote · latencia media", f"{lat_micro.mean() * 1000:,.0f} ms", f"p95 {np.percentile(lat_micro, 95) * 1000:,.0f} ms", delta_color="off")
    b.metric("Evento a evento · latencia media", f"{lat_evento.mean() * 1000:,.0f} ms", delta_color="off")
    st.bar_chart(pd.DataFrame({"Micro-lote": np.sort(lat_micro) * 1000, "Evento a evento": np.sort(lat_evento) * 1000}))

    st.markdown("#### ¿Cuál elegir?")
    r1, r2, r3 = st.columns(3)
    lat = r1.radio("Latencia requerida", ["Minutos", "Segundos", "Sub-segundo"], key="fl_lat")
    equipo = r2.radio("Equipo / stack", ["Python y SQL", "Java/Scala"], key="fl_eq")
    kafka = r3.radio("Datos solo en Kafka y app propia", ["Sí", "No"], key="fl_kafka")
    estado_grande = st.checkbox("Estado grande o lógica compleja de eventos (patrones, joins de flujos)", key="fl_state")
    if lat == "Sub-segundo" and (estado_grande or kafka == "No"):
        rec, why = "Apache Flink", "Necesitas latencia mínima con estado complejo/fuentes variadas y event time avanzado."
    elif kafka == "Sí" and equipo == "Java/Scala" and lat != "Minutos":
        rec, why = "Kafka Streams", "Todo vive en Kafka, tu equipo es JVM y no quieres operar otro clúster."
    elif lat == "Sub-segundo":
        rec, why = "Apache Flink (o Kafka Streams si todo es Kafka)", "La latencia sub-segundo descarta los micro-lotes."
    else:
        rec, why = "Spark Structured Streaming", "Latencia de segundos es suficiente y reutilizas la misma API que en batch (Python/SQL)."
    st.success(f"**Sugerencia: {rec}** — {why}")
