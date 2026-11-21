# Pruebas y evidencias que debe capturar el equipo

Las siguientes pruebas permiten demostrar la ejecución de la ampliación. Las capturas deben tomarse en el equipo donde se ejecute el proyecto; no deben reemplazarse por imágenes simuladas.

1. **Menú integrado:** ejecutar `python ingdatos.py` y capturar la nueva opción 7.
2. **API:** ejecutar opción 7 → opción 1; capturar término consultado, cantidad de registros y archivo guardado en Bronze.
3. **ETL:** ejecutar opción 2; capturar las cantidades de filas Bronze, Silver y Gold y la ruta de los archivos.
4. **NumPy/Pandas:** ejecutar opción 3; capturar media, mediana, desviación estándar, percentil 90 y cantidad de tracks mayores a cinco minutos.
5. **Modelado dimensional:** ejecutar opción 4; abrir `data_lake/gold` y capturar las tablas `dim_artista`, `dim_genero`, `dim_track_estrella`, `dim_track_copo_nieve` y `hecho_catalogo`.
6. **Spark batch:** ejecutar opción 5; capturar el resultado agrupado por artista y la carpeta de salida. Requiere Java y PySpark.
7. **Spark streaming:** ejecutar opción 6; capturar la ejecución, carpeta de salida y carpeta de checkpoint. Requiere Java y PySpark.
8. **Kafka real:** iniciar Docker Compose, ejecutar productor y consumidor; capturar partición, offset y evento. La simulación del menú 6 original debe documentarse por separado como simulación.

## Criterios de aceptación

- No se debe afirmar que iTunes proporciona reproducciones reales de Spotify.
- No se debe llamar Lakehouse transaccional a una carpeta CSV/Parquet sin formato de tabla ACID; describirla como arquitectura por capas tipo Lakehouse.
- Diferenciar el Kafka simulado del broker real de Docker.
- Flink y Kafka Streams se documentan como comparación académica, no como componentes ejecutados.
- Identificar qué integrante realizó cada evidencia y quién revisó el resultado.
