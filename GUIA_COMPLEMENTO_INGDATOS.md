# Guía de ampliación académica — Proyecto de Ingeniería de Datos Spotify

## 1. Propósito

Esta ampliación convierte el proyecto inicial en un caso de estudio de extremo a extremo. Conserva los datasets de Spotify de Kaggle, el análisis con Pandas, el esquema estrella y el módulo educativo de streaming. Añade una extracción complementaria desde una API pública, almacenamiento por capas, un pipeline ETL, análisis con NumPy, modelo copo de nieve, procesamiento batch con Spark, ventanas temporales y una conexión opcional a un broker Kafka real.

**Precisión de los datos:** la API iTunes Search entrega metadatos de catálogo (título, artista, álbum, género, duración y fecha). No entrega reproducciones reales de Spotify. Las métricas de reproducciones deben seguir procediendo de los datasets que sí las contengan.

## 2. Arquitectura propuesta

```text
       Kaggle (datasets Spotify)           API iTunes Search
                  |                               |
                  +---------------+---------------+
                                  v
                       EXTRACCIÓN / INGESTA
                                  |
                                  v
                       DATA LAKE - BRONZE
                         datos originales
                                  |
                                  v
                       TRANSFORMACIÓN ETL
                 Python + Pandas + NumPy / Spark
                                  |
                                  v
                        DATA LAKE - SILVER
                  datos limpios y estandarizados
                                  |
                                  v
                         DATA WAREHOUSE
                 dimensiones + tabla de hechos
                       /                 \
                Modelo estrella       Copo de nieve
                       \                 /
                                  v
                         DATA LAKE - GOLD
                       agregados analíticos

   Eventos de reproducción -> Productor -> Kafka -> Spark Streaming
                                      |                 |
                                   tópico          ventanas / alertas
```

La arquitectura por capas Bronze/Silver/Gold es una organización tipo Lakehouse en este prototipo; no equivale por sí sola a una plataforma Lakehouse transaccional con catálogo, control ACID y formatos de tabla como Delta Lake, Iceberg o Hudi.

## 3. OLTP y OLAP

- **OLTP (Online Transaction Processing):** sistemas operacionales que registran transacciones individuales, por ejemplo, cada reproducción o cambio de estado. Priorizan escrituras frecuentes, consistencia y consultas puntuales.
- **OLAP (Online Analytical Processing):** sistemas orientados al análisis histórico y agregado, por ejemplo, reproducciones por artista, género, periodo o canción.
- **Aplicación al proyecto:** el flujo de eventos representa conceptualmente la captura operacional; las dimensiones y hechos constituyen la estructura analítica OLAP. El proyecto no implementa una base OLTP transaccional completa.

## 4. Data Warehouse, Data Lake y Lakehouse

| Concepto | Característica | Uso en el proyecto |
|---|---|---|
| Data Warehouse | Datos estructurados, integrados y preparados para análisis | Dimensiones y tabla de hechos del esquema estrella |
| Data Lake | Conserva datos de distintas fuentes y formatos, incluso crudos | Carpetas `data_lake/bronze`, `silver` y `gold` |
| Lakehouse | Busca combinar flexibilidad de lake con gestión y rendimiento analítico de warehouse | Se presenta como arquitectura por capas; para Lakehouse completo se recomienda incorporar Delta Lake/Iceberg y catálogo |

## 5. Modelos dimensionales

### 5.1 Estrella

La tabla de hechos se relaciona directamente con dimensiones descriptivas. Para el catálogo musical se usan `hecho_catalogo`, `dim_track_estrella` y `dim_artista`; puede incorporarse `dim_genero`. En el proyecto original, `hecho_streaming` contiene métricas de reproducción cuando la fuente realmente las proporciona.

### 5.2 Copo de nieve

Normaliza dimensiones en subdimensiones. En esta ampliación, `dim_track_copo_nieve` separa referencias de artista y género mediante claves. Esto reduce redundancia, aunque requiere más uniones al consultar.

**Granularidad:** antes de construir una tabla de hechos debe definirse qué representa una fila. En `hecho_catalogo`, una fila representa un track del catálogo; no debe interpretarse como una reproducción individual.

## 6. Python, NumPy, Pandas y DataFrames

- Pandas permite leer, limpiar, agrupar, unir y exportar tablas.
- NumPy permite operaciones vectorizadas y estadísticas numéricas, por ejemplo media, mediana, desviación estándar y percentiles de duración.
- Un DataFrame es una estructura tabular con filas, columnas e índices.
- La función `resumen_numpy_pandas()` convierte duraciones a minutos y calcula estadísticas sin iterar fila por fila.

## 7. APIs y extracción

`pipeline_avanzado.py` consulta iTunes Search API mediante HTTP GET, recibe JSON, normaliza los campos seleccionados y guarda la respuesta en Bronze. La extracción debe documentar endpoint, parámetros, fecha de consulta, campos, errores HTTP y límites del servicio. La API se usa como fuente complementaria de metadatos, no como sustituto de los datasets Spotify.

## 8. ETL y ELT

- **ETL:** extraer, transformar y cargar. El módulo obtiene registros, limpia y estandariza con Pandas y luego guarda las capas procesadas.
- **ELT:** extraer y cargar primero; transformar después dentro del entorno de destino. Puede describirse como alternativa si las transformaciones se ejecutan directamente con Spark sobre Bronze.
- **Pipeline implementado:** extracción → Bronze → limpieza/estandarización → Silver → agregación analítica → Gold.
- Los archivos originales de Kaggle no se sobrescriben.

## 9. Apache Spark: batch y streaming

- **Batch:** `ejecutar_spark_batch()` lee Silver, agrupa por artista y guarda resultados analíticos en Parquet. Se ejecuta en modo local para fines académicos; no es una medición de clúster distribuido.
- **Streaming:** `spark_streaming_ventanas()` observa archivos JSON entrantes, aplica transformaciones y agrega reproducciones por ventanas temporales.
- **Transformations:** operaciones como `filter`, `select`, `withColumn`, `groupBy` y `agg` describen un nuevo DataFrame de forma perezosa.
- **Actions:** operaciones como `show`, `collect`, `count` o el inicio/ejecución de una consulta materializan o ejecutan el trabajo.

## 10. Kafka: topics, brokers, productores y consumidores

El proyecto original incluye una simulación didáctica de `Topico`, `Productor` y `Consumidor`, con particiones, offsets y consumer groups. Esta simulación no es un broker Kafka real.

La ampliación añade `docker-compose.kafka.yml` para levantar un broker Apache Kafka en modo KRaft de un solo nodo y `kafka_real.py` para publicar y consumir JSON en `spotify.reproducciones`.

- **Broker:** servidor que recibe, almacena y entrega registros.
- **Topic:** categoría lógica donde se publican eventos.
- **Partition:** segmento ordenado de un topic; el orden se garantiza dentro de una partición.
- **Producer:** publica eventos, opcionalmente con clave (en el ejemplo, artista).
- **Consumer:** lee eventos y mantiene offsets asociados a su grupo.

Comandos (requiere Docker instalado):

```bash
docker compose -f docker-compose.kafka.yml up -d
pip install kafka-python
python kafka_real.py producir --cantidad 10
python kafka_real.py consumir --cantidad 10
docker compose -f docker-compose.kafka.yml down
```

El broker de un solo nodo es para laboratorio, no para producción. No proporciona alta disponibilidad.

## 11. Event Time, Processing Time, ventanas y eventos tardíos

- **Event Time:** instante en que ocurrió el evento según el dato fuente (`event_time`).
- **Processing Time:** instante en que Spark procesa el registro (`processing_time`).
- **Ventana:** agrupa eventos de un intervalo, por ejemplo ventanas de cinco minutos.
- **Watermark:** umbral que permite a Spark estimar hasta dónde esperar eventos tardíos y limpiar estado de agregaciones. No significa que cualquier evento tardío sea recuperado; los que superen el umbral pueden quedar fuera de resultados append ya emitidos.

La demo configura ventanas de cinco minutos y watermark de diez minutos. La entrada incluye un evento antiguo para ilustrar el concepto; el resultado depende de la hora de ejecución y del avance del watermark.

## 12. Tolerancia a fallos y recuperación

La consulta de Structured Streaming escribe un `checkpointLocation` persistente. Spark guarda allí información de progreso y estado para intentar continuar tras un reinicio compatible. Para que la recuperación sea fiable deben conservarse el checkpoint, la fuente y el destino; no se debe borrar el checkpoint entre reinicios de la misma consulta. El comportamiento exacto depende del origen, sink y garantías de entrega. La demo local no debe describirse como una solución de alta disponibilidad.

Kafka persiste eventos en el broker y mantiene offsets por grupo. La durabilidad y disponibilidad reales dependen de replicación, confirmaciones, retención y configuración. El Compose de laboratorio usa un solo broker y factor de replicación 1.

## 13. Apache Flink y Kafka Streams

Son alternativas de procesamiento de flujos y se documentan como comparación conceptual, no como tecnologías ejecutadas en esta entrega:

| Tecnología | Enfoque | Diferencia principal |
|---|---|---|
| Spark Structured Streaming | Procesamiento de streams integrado al ecosistema Spark, comúnmente con micro-batches | Integra batch y streaming en la misma API de DataFrames |
| Apache Flink | Motor de procesamiento de eventos con foco en streaming y estado | Procesamiento orientado a eventos, event time y estado avanzado |
| Kafka Streams | Biblioteca Java para construir aplicaciones de procesamiento que leen/escriben Kafka | Se integra directamente con topics y particiones Kafka; no requiere clúster de procesamiento separado |

No se afirma que Flink o Kafka Streams estén instalados o implementados en este proyecto.

## 14. Archivos nuevos

- `pipeline_avanzado.py`: API, ETL, capas del Data Lake, NumPy/Pandas, modelos dimensionales, Spark batch y ventanas streaming.
- `kafka_real.py`: productor y consumidor contra Kafka real.
- `docker-compose.kafka.yml`: broker Kafka de laboratorio.
- `data_lake/`: se crea al ejecutar las opciones de extracción y procesamiento.

## 15. Requisitos adicionales

```bash
pip install requests numpy pandas pyarrow kafka-python pyspark
```

Para Spark también se requiere Java compatible con la versión instalada de PySpark. Para Kafka real se requiere Docker Desktop o Docker Engine.

## 16. Limitaciones y trabajo futuro

1. La API iTunes no aporta métricas de reproducción de Spotify.
2. El Data Lake usa archivos Parquet o CSV; no incorpora todavía un formato Lakehouse transaccional (Delta/Iceberg/Hudi).
3. Kafka se ejecuta en un único broker para laboratorio.
4. El conector Spark-Kafka debe configurarse con una versión compatible de Spark/Scala si se desea consumir el topic directamente desde Spark. La función `leer_desde_kafka()` del módulo original es una plantilla y requiere ese conector.
5. Flink y Kafka Streams se comparan conceptualmente, pero no se ejecutan.
6. Se recomienda agregar pruebas automatizadas, control de calidad de datos, catálogo, logging, gestión de secretos y despliegue en clúster como evolución.


## Interfaz gráfica de visualización

Se incorpora `dashboard.py`, desarrollada con Streamlit. La interfaz permite
seleccionar el origen de datos (API iTunes en vivo, archivo local, Kaggle o capas Bronze/Silver). La búsqueda en vivo permite consultar canciones por artista, título o término sin cargar archivos, y seleccionar el número de resultados. La respuesta de API queda disponible en la sesión para su análisis y para ejecutar ETL,
consultar indicadores KPI, visualizar distribuciones por artista y género,
examinar duración y popularidad, filtrar canciones y revisar calidad de datos.
La aplicación no modifica automáticamente los archivos de origen. Los botones
de ETL y modelado ejecutan las funciones de `pipeline_avanzado.py` y guardan
resultados en `data_lake/`.

Para iniciarla:

```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

La visualización detecta nombres de columnas habituales en los datasets del
proyecto. Si un archivo usa encabezados diferentes, se conserva la tabla general,
aunque algunas gráficas específicas pueden no estar disponibles.
