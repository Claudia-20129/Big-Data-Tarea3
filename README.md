# Big-Data-Tarea3
Este proyecto utiliza PySpark para analizar un conjunto de datos de COVID-19 almacenado en un archivo CSV en HDFS. El script realiza diversas operaciones, como la filtración de casos, estadísticas básicas y ordenamiento de datos.

## Descripción de la Solución

El script realiza las siguientes operaciones:

1. **Inicialización de Spark**: Se inicia una sesión de Spark para la manipulación de datos.
2. **Lectura del archivo CSV**: Se carga un archivo CSV desde HDFS.
3. **Análisis de datos**:
   - Se imprime el esquema del DataFrame y las primeras filas del mismo.
   - Se filtran los casos de fallecidos en una ciudad específica (Cali).
   - Se filtran casos con personas mayores de 60 años.
   - Se ordenan los casos por fecha de diagnóstico en orden descendente.
   - Se consulta el número total de casos recuperados.

## Instrucciones para la Ejecución

1. **Instalar PySpark**: Asegúrate de tener PySpark instalado. Puedes instalarlo usando el siguiente comando:
   ```bash
   pip install pyspark

2. **Configurar HDFS**: Verifica que HDFS esté en funcionamiento y que el archivo CSV (qt2j-8ykr.csv) esté disponible en la ruta especificada:
    ```bash
   hdfs://localhost:9000/Tarea3/

3. **Ejecutar el script**:
   Guarda el siguiente código en un archivo llamado tarea3.py.
   Ejecuta el script usando el siguiente comando en tu terminal:
     spark-submit tarea3.py
   

   4. ## Código Fuente
  
   # Importamos las librerías necesarias
    from pyspark.sql import SparkSession, functions as F

# Inicializamos la sesión de Spark
    spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Definimos la ruta del archivo .csv en HDFS
    file_path = 'hdfs://localhost:9000/Tarea3/qt2j-8ykr.csv'

# Leemos el archivo .csv
    df = spark.read.format('csv').option("header", 'true').option('inferSchema', 'true').load(file_path)

# Imprimimos el esquema
    df.printSchema()

# Mostramos las primeras filas del DataFrame
    df.show(10)

# Estadísticas básicas
    df.describe().show()

# Filtrar los casos por ciudad (por ejemplo: "CALI") y estado (por ejemplo: "Fallecido")
    print("Casos fallecidos en Cali\n")
    casos_cali_fallecidos = df.filter(
    (F.col('ciudad_municipio_nom') == 'CALI') & 
    (F.col('estado') == 'Fallecido')
    )
    casos_cali_fallecidos.show()

# Filtrar casos con edad mayor a 60 años
    print("Casos con personas mayores de 60 años\n")
    mayores_60 = df.filter(F.col('edad') > 60).select('edad', 'sexo', 'estado', 'recuperado', 'fecha_diagnostico')
    mayores_60.show()

# Ordenar filas por la fecha de diagnóstico en orden descendente
    print("Casos ordenados por fecha de diagnóstico\n")
    casos_ordenados_fecha = df.sort(F.col('fecha_diagnostico').desc())
    casos_ordenados_fecha.show(10)

# Consulta adicional: Número de casos recuperados
    print("Número de casos recuperados\n")
    recuperados = df.filter(F.col('recuperado') == 'Recuperado').count()
    print(f"Total de casos recuperados: {recuperados}")
