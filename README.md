# Proyecto de Ingeniería de Datos — Spotify

Aplicación de consola en Python que descarga dos datasets de Spotify desde Kaggle, permite explorarlos, los transforma a un **esquema estrella** (modelo de Data Warehouse) y los **fusiona en un esquema unificado**. Incluye herramientas para medir y liberar el **uso de memoria**, para detectar y limpiar **duplicados**, y un módulo de **procesamiento en tiempo real** (Apache Kafka y Spark Structured Streaming).

## Contenido

- [Estructura del proyecto](#estructura-del-proyecto)
- [Requisitos e instalación](#requisitos-e-instalación)
- [Cómo ejecutarlo](#cómo-ejecutarlo)
- [Datasets utilizados](#datasets-utilizados)
- [Qué puede hacer el programa (menús)](#qué-puede-hacer-el-programa-menús)
- [Modelo de datos (esquema estrella)](#modelo-de-datos-esquema-estrella)
- [Cómo se fusionaron los datasets](#cómo-se-fusionaron-los-datasets)
- [Duplicados y limpieza de datos](#duplicados-y-limpieza-de-datos)
- [Streaming en tiempo real (Kafka y Spark)](#streaming-en-tiempo-real-kafka-y-spark)
- [Manejo de memoria](#manejo-de-memoria)
- [Mapa de funciones](#mapa-de-funciones)
- [Limitaciones conocidas](#limitaciones-conocidas)

## Estructura del proyecto

```
ProyectoDatos-master/
├── ingdatos.py     # Programa principal: carga, exploración, esquema estrella, fusión, duplicados, memoria y menús
├── streaming.py    # Kafka simulado y Spark Structured Streaming
├── pipeline_avanzado.py # API, ETL por capas, NumPy/Pandas, modelos dimensionales y Spark
├── kafka_real.py   # Productor/consumidor para broker Kafka real
├── docker-compose.kafka.yml # Broker Kafka de laboratorio
├── GUIA_COMPLEMENTO_INGDATOS.md # Fundamentos y guía de los temas ampliados
├── README.md       # Este archivo
└── .idea/          # Configuración del IDE (PyCharm), no es parte de la lógica
```

## Requisitos e instalación

- Python 3.9 o superior
- `pandas`
- `kagglehub` (obligatoria; si falta, el programa avisa y se cierra)
- `psutil` (opcional; permite medir la RAM actual del proceso)
- `pyspark` y **Java** (opcionales; solo para las opciones de Spark Structured Streaming del menú de streaming). Spark necesita un JDK compatible con tu versión de PySpark.

```bash
pip install pandas numpy requests pyarrow "kagglehub[pandas-datasets]" psutil pyspark kafka-python
```

Sin `pyspark` o sin Java todo lo demás funciona; las opciones de Spark avisan qué falta. Para Kafka real se necesita Docker y `kafka-python`.

Los datasets se descargan desde Kaggle, por lo que se necesita conexión a internet. Según tu configuración, `kagglehub` puede pedir credenciales de Kaggle.

## Cómo ejecutarlo

```bash
python ingdatos.py
```

La primera vez descarga el dataset; las siguientes reutiliza la copia local de `kagglehub`. Dentro del programa, los datos ya leídos se guardan en una caché en memoria para no volver a leerlos.


## Ampliación: pipeline avanzado y arquitectura Lakehouse

Además del menú original, se incorpora un segundo módulo independiente para trabajar los temas avanzados de la asignatura:

```bash
python pipeline_avanzado.py
```

Incluye extracción de metadatos desde iTunes Search API, capas Bronze/Silver/Gold, ETL con Pandas, análisis vectorizado con NumPy, modelos estrella y copo de nieve, procesamiento batch con Spark y ventanas temporales con Event Time, Processing Time, Watermark y checkpoint. Los datos generados se guardan en `data_lake/`. La API iTunes aporta metadatos, no reproducciones reales de Spotify.

Para probar un broker Kafka real (opcional):

```bash
docker compose -f docker-compose.kafka.yml up -d
pip install kafka-python
python kafka_real.py producir --cantidad 10
python kafka_real.py consumir --cantidad 10
```

La guía conceptual completa, incluyendo OLTP/OLAP, Data Warehouse/Data Lake/Lakehouse, ETL/ELT y comparación Spark/Flink/Kafka Streams, está en `GUIA_COMPLEMENTO_INGDATOS.md`. Flink y Kafka Streams se documentan como alternativas conceptuales; no se presentan como implementados.

## Datasets utilizados

| Clave | Dataset | Handle en Kaggle |
|-------|---------|------------------|
| 1 | Spotify Dataset 1921-2020 (600k Tracks) | `yamaerenay/spotify-dataset-19212020-600k-tracks` |
| 2 | Spotify200 (Top 200 semanal) | `yelexa/spotify200` |

Si un dataset trae varios archivos (`.csv`, `.json` o `.parquet`), el programa muestra la lista y deja elegir cuál cargar. Si trae uno solo, lo carga directamente.

## Qué puede hacer el programa (menús)

**Menú principal**

| Opción | Acción |
|--------|--------|
| 1 | Abrir el dataset Spotify 1921-2020 |
| 2 | Abrir el dataset Spotify200 |
| 3 | Ver ambos datasets juntos (esquema unificado) |
| 4 | Ver uso de memoria del proyecto |
| 5 | Liberar memoria (vaciar caché de datasets) |
| 6 | Streaming en tiempo real (Kafka + Spark Structured Streaming) |
| 0 | Salir |

**Menú de un dataset individual**

| Opción | Acción |
|--------|--------|
| 1 | Información general: filas, columnas, tipos y primeros 5 registros |
| 2 | Estadísticas descriptivas de las columnas numéricas |
| 3 | Valores nulos por columna |
| 4 | Estructura tipo Data Warehouse (submenú del esquema estrella) |
| 5 | Ver duplicados (filas idénticas y canciones repetidas) |
| 6 | Limpiar datos (espacios, vacíos, filas sin título y duplicados) |
| 7 | Uso de memoria de este dataset (total y por columna) |
| 8 | Volver |

**Submenú del esquema estrella (individual y unificado)**

| Opción | Acción |
|--------|--------|
| 1 | Ver `dim_tiempo` |
| 2 | Ver `dim_artista` |
| 3 | Ver `dim_track` |
| 4 | Ver `hecho_streaming` |
| 5 | Uso de memoria de las tablas generadas |
| 6 | Volver |

**Menú del esquema unificado (opción 3 del menú principal)**

| Opción | Acción |
|--------|--------|
| 1 a 4 | Ver `dim_tiempo`, `dim_artista`, `dim_track` y `hecho_streaming` unificados |
| 5 | Ver duplicados de cada dataset y entre ambos |
| 6 | Limpiar ambos datasets |
| 7 | Uso de memoria de las tablas generadas |
| 8 | Volver |

Las tablas se construyen solo cuando se piden por primera vez y quedan guardadas mientras estés en ese menú.

## Modelo de datos (esquema estrella)

```
                dim_tiempo
                    │
 dim_artista ── dim_track ── hecho_streaming
```

### dim_tiempo
Una fila por fecha única.
`tiempo_key`, `fecha`, `anio`, `mes`, `nombre_mes`, `dia_mes`, `dia_semana`, `nombre_dia_semana`, `trimestre`, `es_fin_de_semana`.

### dim_artista
Una fila por artista único.
`artista_key`, `nombre_artista`, `artista_id` (formato `ART-1`, `ART-2`, ...).

### dim_track
Una fila por canción (título + artista).
`track_key`, `titulo_track`, `duracion_total_ms`, `genero_musical`, `popularidad_actual`, `artista_key` (solo en la versión unificada), `track_id` (formato `TRK-1`, ...).

### hecho_streaming
Una fila por registro de reproducciones.
`streaming_id`, `track_key`, `tiempo_key`, `cantidad_reproducciones` y, solo en la versión unificada, `dataset_origen`.

### Campos del diseño original que no existen en los datos
Los datasets no traen esta información, por lo que esos campos se omiten (y el código lo deja indicado en comentarios):
`sello_discografico`, `pais_artista`, `idioma`, `hora`, `tiempo_reproducido_ms`, `reproduccion_completa_flag`, `usuario_key`, `dispositivo_key`, `contexto_key`.

## Cómo se fusionaron los datasets

La opción 3 del menú principal construye un esquema único a partir de los dos datasets. Los nombres de columnas son distintos entre ellos, así que la fusión sigue estos pasos:

### 1. Detectar las columnas equivalentes
`buscar_columna()` busca, sin distinguir mayúsculas, la primera columna que coincida con una lista de nombres posibles:

| Concepto | Nombres que acepta |
|----------|--------------------|
| Artista | `artists`, `artist`, `artist_name`, `nombre_artista` |
| Título de canción | `name`, `track_name`, `title`, `titulo_track` |
| Fecha | `date`, `release_date`, `week`, `chart_week`, `fecha` |
| Reproducciones | `streams`, `playcount`, `cantidad_reproducciones` |
| Duración | `duration_ms`, `duracion_total_ms` |
| Género | `genre`, `track_genre`, `genero_musical` |
| Popularidad | `popularity`, `popularidad_actual` |

Si un dataset no tiene alguna columna, simplemente no aporta ese dato.

### 2. Limpiar y normalizar
- `limpiar_artista()`: convierte valores como `"['Nombre']"` en `Nombre`. Si hay varios artistas, conserva el primero.
- `normalizar()`: pasa a minúsculas y quita espacios extra. Se usa para generar claves de comparación, de modo que `"Shape of You "` y `"shape of you"` cuenten como la misma canción.

### 3. Construir cada tabla combinada

| Tabla | Cómo se fusiona |
|-------|-----------------|
| `dim_artista` | Une los artistas de ambos datasets, elimina vacíos y duplicados por nombre normalizado, y asigna `artista_key` y `artista_id` nuevos. |
| `dim_track` | Une las canciones de ambos datasets y las agrupa por título + artista normalizados. Para cada grupo toma el primer valor no nulo de duración, género y popularidad, de modo que un dataset puede completar lo que le falta al otro. Luego asigna `artista_key` buscando el artista en `dim_artista`, y elimina las columnas que quedaron totalmente vacías. |
| `dim_tiempo` | Une las fechas de ambos datasets, deja las fechas únicas ordenadas y calcula año, mes, trimestre, día de la semana, fin de semana, etc. |
| `hecho_streaming` | Recorre cada dataset que tenga columna de reproducciones y de título. Une todas las filas en una sola tabla, agrega la columna `dataset_origen`, y relaciona cada fila con `track_key` (por título normalizado) y `tiempo_key` (por fecha). Descarta las filas cuya canción no aparece en `dim_track`. |

### Resultado de la fusión
- Cada artista y cada canción aparece **una sola vez**, aunque esté en los dos datasets.
- La información complementaria de cada fuente se combina en la misma fila de `dim_track`.
- Los datasets sin métrica real de reproducciones (por ejemplo, el de 600k tracks si no trae `streams`) aportan a las dimensiones, pero no al hecho. El programa lo avisa por consola.

**Menú de streaming (opción 6 del menú principal)**

| Opción | Acción |
|--------|--------|
| 1 | Conceptos: batch vs. streaming, latencia, Kafka y Spark |
| 2 | Pedidos: análisis de la actividad (conteo por zona, mayor tiempo, regla de alerta) |
| 3 | Pedidos: respuestas de la actividad (decisión técnica, arquitectura, diagrama, conclusión) |
| 4 | Pedidos: flujo con Kafka simulado (productor → tópico → consumidor) |
| 5 | Pedidos: Spark Structured Streaming real |
| 6 | Spotify: flujo con Kafka simulado (eventos generados desde un dataset) |
| 7 | Spotify: Spark Structured Streaming real |
| 8 | Volver |

## Duplicados y limpieza de datos

### Conteo de duplicados
- **Filas duplicadas exactas:** filas con todas las columnas iguales a otra anterior (la primera aparición no cuenta). Se muestra la cantidad y el porcentaje.
- **Canciones repetidas:** canciones únicas (título + artista normalizados) y cuántos registros repiten una canción ya vista. En un ranking semanal como Spotify200 esto es normal, porque una canción aparece en varias semanas; no es un error.
- **Entre ambos datasets** (menú unificado): canciones únicas de cada dataset, cuántas están en los dos y cuántas quedan tras la fusión. Esas canciones comunes son las que la fusión une en una sola fila de `dim_track`.

### Limpieza (`limpiar_dataframe`)
Se aplica a cada dataset por separado y el resultado reemplaza la copia en memoria, de modo que el esquema estrella usa los datos ya limpios. Pasos, en este orden:

1. Quitar espacios sobrantes al inicio y al final de los textos, y convertir textos vacíos en nulos.
2. Eliminar filas completamente vacías.
3. Eliminar filas sin título de canción (si el dataset tiene esa columna).
4. Eliminar filas duplicadas exactas.

Al terminar muestra cuántas filas y valores se tocaron en cada paso, las filas antes y después, los nulos totales y la memoria antes y después. La limpieza trabaja sobre los datos en memoria; los archivos originales descargados no se modifican.

## Streaming en tiempo real (Kafka y Spark)

Módulo `streaming.py`, accesible desde la opción 6 del menú principal (o solo con `python streaming.py`). Aplica los temas de las últimas clases: procesamiento por lotes vs. continuo, latencia, eventos, Apache Kafka y Spark Structured Streaming.

### Batch vs. streaming
| | Batch | Streaming |
|---|-------|-----------|
| Entrada | Conjunto delimitado (archivo, tabla) | Flujo potencialmente ilimitado de eventos |
| Ejecución | Programada o cuando el lote está disponible | Consulta siempre activa |
| Latencia | Minutos, horas o días | Baja, según carga e infraestructura |
| Útil para | Informes históricos, conciliaciones, cierres | Alertas, monitoreo, métricas operativas |

El proyecto es **batch** en su parte principal (se leen archivos completos de Kaggle). El módulo de streaming muestra la otra forma de procesar los mismos datos, tratando cada registro como un evento.

### Kafka (simulado)
Las clases `Topico`, `Productor` y `Consumidor` reproducen en Python el modelo de Kafka, **sin necesidad de instalar ni levantar un broker**:
- **Tópico con particiones:** con clave, la misma clave va siempre a la misma partición (se conserva su orden); sin clave se reparte por turnos.
- **Offsets:** cada evento tiene su posición dentro de la partición.
- **Consumer group:** consumidores del mismo grupo comparten el offset (no leen dos veces el mismo evento); un grupo distinto lee todo desde el inicio.

Es una **simulación educativa**: no tiene red, persistencia ni tolerancia a fallos como Kafka real. Para conectar Spark a un Kafka real, `leer_desde_kafka()` deja la plantilla de la fuente (necesita un broker y el conector `spark-sql-kafka` con la versión que coincida con tu Spark; **no se probó** porque requiere ese entorno).

### Spark Structured Streaming (real)
Se ejecuta Spark en modo local leyendo una carpeta a la que el programa va **dejando archivos JSON nuevos**, como en los ejemplos de la clase:

```
carpeta de entrada (JSON nuevos) → readStream + esquema → filter / groupBy → writeStream (memory sink) → resultados
```

| Consulta | Qué calcula | Modo de salida |
|----------|-------------|----------------|
| `entregados` | Filtra pedidos con estado "Entregado" | append |
| `entregados_por_zona` | Cuenta de pedidos entregados por zona | complete |
| `tiempo_maximo` | Mayor tiempo de entrega | complete |
| `alertas_mas_de_45_min` | Entregados con más de 45 minutos | append |

Al llegar cada archivo, el programa espera a que Spark lo procese y muestra cómo se actualiza cada resultado. Notas técnicas: el esquema se declara (en lectura de archivos no se infiere); los agregados mantienen estado y se usan `outputMode("complete")` para ellos y `append` para los filtros.

### Actividad: monitoreo de pedidos de domicilios
Datos de la actividad (el tiempo `0` significa "aún no entregado", no es un tiempo real):

| Pedido | Zona | Estado | Tiempo (min) |
|--------|------|--------|--------------|
| P001 | Norte | Entregado | 25 |
| P002 | Centro | En preparación | 0 |
| P003 | Norte | Entregado | 40 |
| P004 | Sur | Entregado | 32 |
| P005 | Centro | Entregado | 50 |
| P006 | Norte | En camino | 0 |

**01 · Decisión técnica.** Streaming. Consultar los cambios de estado sin esperar al cierre del día exige baja latencia; batch acumula y responde en minutos, horas o días. Escenario adecuado para batch en la misma plataforma: calcular al cierre del día los ingresos y pedidos totales por zona, o conciliar pagos con repartidores.

**02 · Arquitectura con Kafka.**

| Componente | Papel |
|------------|-------|
| Productor | Aplicación de domicilios: publica un evento en cada cambio de estado |
| Tópico | `pedidos.estado` (clave = pedido, para conservar el orden de cada pedido) |
| Consumidor | Spark Structured Streaming (consumer group `monitoreo-pedidos`) |
| Salida | Indicadores por zona y alertas de entregas de más de 45 minutos |

**03 · Análisis.**
- Entregados por zona: Norte 2, Centro 1, Sur 1.
- Mayor tiempo de entrega: P005 (Centro), 50 minutos.
- Regla de alerta: `estado == "Entregado"` **y** `tiempo_entrega_min > 45`. Se exige el estado porque el 0 de los pedidos en curso no es un tiempo real. Alerta: P005.
- Acción del sistema: publicar la alerta en `pedidos.alertas` y notificar a operaciones con pedido, zona y tiempo para revisar la causa.

**04 · Flujo de datos.**

```
Aplicación de domicilios ──▶ Apache Kafka ──▶ Spark Structured Streaming ──▶ Indicadores y alertas
      (Productor)           (tópico pedidos.estado)      (Consumidor)                (Salida)
```

**Conclusión.** El procesamiento continuo permite detectar una demora mientras el pedido está en curso y actuar sobre él, y mantiene los indicadores por zona al día sin esperar a un proceso de cierre: reduce la latencia entre el evento y la decisión.

### Spotify como flujo de eventos
`generar_eventos_spotify()` toma una muestra de 30 filas del dataset elegido y las convierte en eventos de reproducción (`evento_id`, `cancion`, `artista`, `genero`, `reproducciones`, `evento_ts`). Si el dataset tiene columna de reproducciones (como Spotify200) se usa su valor; si no (como el de 600k tracks), cada evento cuenta como 1 reproducción.

Para practicar los problemas típicos de un flujo real, **se repiten a propósito ~10 % de los eventos** (mismo `evento_id`), simulando mensajes duplicados. Ambas versiones los descartan:
- **Kafka simulado:** el consumidor guarda los `evento_id` ya vistos y descarta los repetidos, procesando en micro-lotes de 10 eventos con un estado (total por artista).
- **Spark:** `withWatermark("ts", "10 minutes")` + `dropDuplicates(["evento_id", "ts"])`; el watermark limita cuánto estado de deduplicación conserva Spark, el problema de crecimiento del estado que se vio en clase.

Salidas: reproducciones por artista (se actualizan con cada lote) y alertas de eventos cuyas reproducciones superan el percentil 90 de la muestra. Con las mismas semillas, el Kafka simulado y Spark dan los mismos totales.

## Manejo de memoria

Funciones disponibles desde los menús:

| Función | Qué hace |
|---------|----------|
| `mostrar_memoria_dataframe()` | Total de un DataFrame y detalle por columna (tipo, tamaño y % del total), de mayor a menor. Usa `memory_usage(deep=True)` para contar también el contenido de los textos. |
| `mostrar_memoria_tablas()` | Tamaño de las tablas del esquema estrella ya construidas, con total. |
| `mostrar_memoria_proyecto()` | Memoria de cada dataset cacheado, total y RAM del proceso. |
| `liberar_memoria()` | Vacía las cachés de datasets y llama a `gc.collect()`; muestra la RAM antes y después. |
| `memoria_proceso()` | Con `psutil`: RAM actual (RSS). Sin `psutil`: RAM máxima alcanzada (módulo `resource`, solo Linux/macOS). |

## Mapa de funciones

| Sección en `ingdatos.py` | Funciones |
|--------------------------|-----------|
| Carga de datos | `descargar_dataset`, `listar_archivos`, `elegir_archivo`, `leer_archivo`, `cargar_dataframe` |
| Exploración | `mostrar_info_general`, `mostrar_estadisticas`, `mostrar_nulos` |
| Uso de memoria | `_fmt_bytes`, `memoria_proceso`, `mostrar_memoria_dataframe`, `mostrar_memoria_tablas`, `mostrar_memoria_proyecto`, `liberar_memoria` |
| Duplicados y limpieza | `mostrar_duplicados`, `mostrar_duplicados_combinados`, `limpiar_dataframe` (y auxiliares `_contar_filas_duplicadas`, `_clave_cancion`, `_artista_limpio`) |
| Streaming (`streaming.py`) | `mostrar_conceptos`, `Topico`, `Productor`, `Consumidor`, `analizar_pedidos`, `mostrar_respuestas_actividad`, `simular_pedidos_con_kafka`, `generar_eventos_spotify`, `simular_spotify_con_kafka`, `spark_streaming_pedidos`, `spark_streaming_spotify`, `leer_desde_kafka`, `menu_streaming` |
| Utilidades de fusión | `buscar_columna`, `limpiar_artista`, `normalizar`, `_mostrar_tabla` |
| Esquema estrella individual | `construir_dim_tiempo`, `construir_dim_artista`, `construir_dim_track`, `construir_hecho_streaming` |
| Esquema estrella unificado | `construir_dim_artista_combinado`, `construir_dim_track_combinado`, `construir_dim_tiempo_combinado`, `construir_hecho_streaming_combinado` |
| Menús | `menu_estructura`, `menu_dataset`, `menu_combinado`, `menu_principal` |

## Limitaciones conocidas

- **Relación del hecho por título:** `hecho_streaming` se une a `dim_track` solo por título normalizado. Si hay canciones distintas con el mismo título, la unión puede **duplicar filas**. Una mejora es unir por título + artista.
- **Un solo artista por canción:** cuando hay varios artistas, solo se conserva el primero.
- **Dato del primer valor:** al fusionar canciones repetidas se toma el primer valor no nulo, sin criterio de prioridad entre datasets.
- **Carga completa en RAM:** los archivos se leen enteros con pandas, sin optimizar tipos ni leer por partes. Con datasets grandes (como Spotify200) puede consumir mucha memoria.
- **Kafka es simulado:** el flujo de Kafka del menú vive en memoria de un solo proceso. Spark sí es real, pero lee de una carpeta de archivos JSON y no de un broker Kafka.
- **Spark en demos pequeñas:** se ejecuta en modo local con pocos eventos; no mide rendimiento real de un clúster.
- **Sin persistencia:** las tablas del esquema estrella se reconstruyen en cada ejecución y no se exportan a archivos.


## Interfaz gráfica de visualización (Dashboard)

El proyecto incluye `dashboard.py`, una interfaz web local construida con Streamlit.
Ahora permite consultar en vivo la API pública de iTunes (sin subir archivos), buscando por artista, canción o término y eligiendo la cantidad de resultados. También conserva la carga opcional de CSV/Parquet/JSON, las capas Bronze/Silver, los datasets de Kaggle y las visualizaciones de indicadores, artistas, géneros, duración, popularidad y calidad de datos. La consulta API se almacena en la sesión y puede enviarse al ETL para persistir las capas del Data Lake.

### Ejecución

```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

Streamlit abrirá el dashboard en el navegador (normalmente `http://localhost:8501`).
Por defecto, selecciona **API iTunes · Búsqueda en vivo**, escribe un término (por ejemplo, un artista) y presiona **Obtener datos desde API**. La consulta no requiere cargar CSV. Los botones permiten
ejecutar el ETL y construir los modelos dimensionales cuando la fuente contiene
las columnas requeridas. Las gráficas se adaptan a las columnas disponibles.
