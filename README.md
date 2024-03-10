### Aterrizando en el Big Data Porcessing, Spark con Scala y Python 

Así me veo por mi mala cabeza ... inscrito en [Keepcoding : Bootcamp en Big Data, Data Science, Machine Learning e IA](https://keepcoding.io/nuestros-bootcamps/full-stack-big-data-machine-learning-bootcamp/) 

Gran temario, excelente formato y mejores profesores... pero duro.

Pues lo dicho llegando al módulo de Big Data Processing se contempla la necesidad de resolver una práctica final, y las condiciones para dar respuesta, han de pasar por la premisa de utilizar Spark, en cualquiera de sus ecosistemas de instalación y utilizando cualquiera de sus lenguajes de desarrollo.

**Apache Spark** es un framework de análisis de datos, que procesa grandes cantidades de datos, tanto en memoria (tiempo real) como en disco (batch). Es muy escalable, extensivo y con gran calidad de cálculo, siendo mas rápido que Apache Hadoop pudiendo ejecutarse de forma independiente de este y también en clústeres Haddop. 

Su funcionamiento es en clúster jerárquico, maestro-esclavo. El maestro distribuye y supervisa las ejecuciones y consultas, determinando que tareas se ejecutan en qué nodos y en qué secuencia, creando a su vez los **RDD**, colección de objetos distribuidos e inmutables de bajo nivel y tolerantes a fallos. Estos RDD se pueden crear a partir de **DataFrames y Datasets**, texto simples, bases de datos SQL, almacenes NoSQL (como MongoDB..), y buckets. Procesando como  API y orientado a objetos que mantiene una estructura uniforme para los lenguajes de programación permitidos.

Para ejecutarse una maquina local o una maquina virtual, Spark además de su propio framework necesita de una maquina virtual de Java y si es en modo clúster en en cada uno de los nodos del clúster, pero en entornos de producción la tendencia mayoritaria es la utilización de entornos de gestión de recursos AMS, Azure, GCD...  o la implementación mediante Kubernetes. Una variante de estas últimas y muy utilizada es **Databricks**, que además de ofrecer servicios de gestión integral de clúster Apache Spark, tiene un entrono web completo para implementar el desarrollo, dando acceso a HDFS, file browser y database Tables.

La capacidad de procesamiento de spark ha hecho que sea muy valorado y utilizado en el ML, DL y IA, permitiendo desarrollar en varios lenguajes a través de su API donde están incluidos, Java, Scala, Python y R, con SQL como lenguaje de generación de consultas. 

#### La práctica

Después de este "genérico" resumen, vamos a la práctica en ella nos piden que a partir de dos datasets sobre el índice de felicidad de los paises en el transcurso de los años se respondan a una serie de preguntas.  

##### **Pirmera decisión: ¿Que lenguaje de desarrollo emplear?**

- R
- Python
- Scala
- Java

##### Primera conclusión

Como esto va de aprender y demostrase a uno mismo que ha aprendido, la practica quedará resuelta en **Python y en Scala**.

Python con el IDE de Visual Studio Code y Scala Con jetBrians y su Intellij comunity Edition (version gratuita)



##### Segunda decisión es que ecosistema utilizamos 

Estas son algunas de las opciones que tenemos :

- **Servicios gestionados en la nube, cloud computing, Azure, Google, AWS..** 

  Si en el módulo anterior de bootcamp (Arquitectura Big Data) nos centramos en la plataforma Google para practicar y conocer gran parte de los servicios Cloud de Google, ya sea creación de Clústeres Hadoop, de datos Sql o NoSql (posgresSQL, MongoDb, elastic, Redis, Maquinas Virtuales, generación de jobs de dataproc, engine, configuración de firewalls y más, en este nos dedicamos a implantar un clúster spark en Azure. 

  Todas estas plataformas tiene acceso gratuito temporal o por por un número de megas computadas,  por lo tanto factible para hacer un ejercicio como este.

  Ventajas: Muy potente y confiable y de carácter generalista, hay servicios y posibilidades para hacer todo.

  Desventajas: Es de pago al cumplir los requisitos. 

- **Databricks**

  Todo y ofrecer los servicios de cloud, he querido indexarlo separado, por que ofrece lo de los demás, pero tiene un servicio orientado a spark que com o ya explique tiene un entorno completo par su explotación.

  Ventajas: Al tener todo el entrono te permite por ejemplo cambiar de lenguaje en tus notebooks, hacer uso de hdfs con solo copiar y pegar la ruta y despreocuparte de la gestión.

  Desventajas: Es de pago, y en la versión gratuita el clúster creado puede dejar de funcionar en cualquier momento, pero no te impide crearlo cuantas veces necesites y los que necesites. El IDE de desarrollo no es personalizable o completo, como puede ser un Visual Studio Code, un jetBrains o cualquier otro

- **Maquinas virtuales.**

  Poder crear una maquina virtual en local con una instalación mediante docker, es una buena opción y como aplicamos en el módulo, factible sin grandes complejidades.

  Ventajas: Instalación limpia y fácil después de su uso de eliminar, y puede ser tanto en Windows, Mac o Linux. 

  Desventajas: A mi modo de ver, una MV en tu propia maquina, siempre le resta recursos a esa aplicación, pues desde un principio se dimensiona, y nunca es el total de la capacidad de la maquina.

- **Instalación local**

  Esta en mi ha sido en windows, y bueno con algunas "complejidades" propias de windows en cuanto a permisos y variables, es factible.

  Ventajas: La puedes tener y usar el tiempo que quieras, no deja muchos rastros a la hora de desinstalar y te permite usar cualquier lenguaje de los admitidos. Pudiendo hacer uso de entornos virtuales. Poder usar el IDE personal para el desarrollo.

  Desventajas: El mantenimiento de nuevas versiones, tanto de Spark como de la maquina virtual de Java. Como se puede inferenciar, es una sola maquina

- **Colaboratory de colab**

  Es la mezcla de los servicios de cloud y maquina virtual local, ya que es usar una maquina virtual en la nube que ejecuta el entorno de spark.

  Ventajas: Desasistido, no hace falta preocuparse por configurar, dimensionar y borrar tras el uso. Es gratuito

  Desventajas: No es un entorno permanente, además de que cada ocasión que te pones a trabajar debes reconstruir el entorno y suele emplear mucho tiempo en preparar la maquina y spark. Además el ide de desarrollo.

##### Segunda conclusión

La verdad es que con todo montado y probado, gracias al módulo y si fuese un entorno profesional, databricks para Spark o Google cloud Services de manera mas generalista, serian las soluciones que emplearía.  

Pero para la práctica o para trabajos menos intensivos, mi decisión ha sido utilizar **un colaboratory** para evaluar su uso con un desarrollo **en python con pySpark** y lo he **terminado en local** con la misma intención de evaluación y para usar un entorno virtual y un IDE personalizado. Y para el desarrollo **en Scala** una cuenta en **Databricks**, aun no tengo la suficiente experiencia con el ide intellij y por tanto tener todo dispuesto en databricks me supone de una gran ayuda. 

#### El código

Estas deben estar sustentadas en el analisis de los datos suministrados , que son son ficheros (world-happiness-report-2021.csv y world-happiness-report.csv). Yo he añadido un fichero más, sobre la relación de continentes y países (list-of-countries-by-continent-2024.csv), para aumentar la calidad de los datos. 

Los pasos a seguir en el flujo de desarrollo son:

1. Importación de los módulos 
2. Crear sesión de spark
3. Crear el objeto de spark session
4. 
5. Leer el dataset
6. 

##### Python y pySpark

Flujo de desarrollo:

Todos los métodos que he empleado tienen su reflejo python respecto a Scala, con sus pequeñas diferencias naturalmente, pero el flujo sería el mismo. Los trozos de código aquí mostrados son en python, pero se facilitan todos los notebooks utilizados.

Y la diferencia de implementar en Databricks, respecto de otros ecosistemas, es que no es necesario el punto1 y 2 de la siguiente lista. 

1. Comprobar que spark esta disponible.

​	Para ello importamos el módulo findspars delk Core de spark

​	**Spark Core** biblioteca principal de motor de spark donde residen el resto de bibliotecas de spark, Spark SQL, Streaming, MLlib, GraphX y la API de pandas Spark

2. Creamos la sesión de spark

   Desde la librería de **Spark SQL** creamos la sesión.

   **Spark SQL** biblioteca que permite usar RDD como consultas SQL, generando Dataframes consultables a través de SQL o otras API de Dataframe como pandas por ejemplo. También permite la consulta mediante SQL de los storage de datos en Hive.  

   ```py
   ### verificar la instalación ###
   import findspark
   findspark.init()
   ### crear la sesion ###
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("whr_spark").master("local[*]").getOrCreate()
   spark
   ```

3. A partir de aquí ya es leer los datos y utilizar las Funciones incorporadas en el momento apropiado.

   Spark como ya se ha dicho puede leer muchos tipos de datos, en este caso solo csv, per la recomendación siempre va ha ser ver tu origen de datos y entender que dato hay, en nel caso de los datos estructurados es importante que al leer esos datos tengan el tipo real, y para asegurarnos de ellos, Spark nos permite crear un esquema de datos para que ya cumplan esa transformación en el momento de la lectura . 

4. Limpieza y transformación de los datos. Algunas precauciones a tener en cuenta.

   **La lectura de las columnas** las hace de derecha a izquierda y utiliza índices empezando por cero, i no hace cargas parciales, por lo tanto si en el esquema nos saltamos alguno de los campos sin definir, le aplicará el de siguiente.

   ```python
   # Leer de fichero csv con esquema
   schema_countries = StructType() \
         .add("country",StringType(),True) \
         .add("region",StringType(),True)
   
   df_countries = spark.read.format("csv") \
         .option("header", True) \
         .schema(schema_countries) \
         .load('Datasets/list-of-countries-by-continent-2024.csv')
   ```

   Las cabeceras o nombres de las columnas pueden tener espacios en blanco, cosa que no se lleva bien con las sentencias SQL, y además en este caso no necesitamos todas las columnas. Con un `select` podemos elegir, **seleccionar** las columnas, teniendo en cuenta que el orden que les demos en esa selección, será la que marque el índice de las columnas y seguidamente **renombrar** estas.

   ```python
   # quedarse con solo algunas y resombrarlas
   df_whr = df_whr.select('Country name','Life Ladder','Log GDP per capita','Healthy life expectancy at birth','year')
   df_whr = df_whr.toDF(*("country", "ladder","gdp","healthy", "year"))
   ```

   **Añadir columnas nuevas** con valores constantes mediante el método `lit()`

   ```python
   # Añadir el año de los datos en una columna
   df_whr2021 = df_whr2021.withColumn("year", F.lit(2021))
   ```

   Como tenemos dos fuetes que contiene los mismos datos en momentos diferentes (años) nos facilitará las consultas que estén en un único dataframe y se una de manera vertical, una **Unión** resolverá la necesidad. 

   ```python
   # Unir los dataframes ahora que tienen la misma estructura y tendremos todos los datos de todos los años
   df_whr_all = df_whr.union(df_whr2021)
   ```

   Si tenemos la necesidad, como es el caso de unir dos dataframe de manera horizontal, una forma seria un `join` , si el campo tiene el mismo nombre en ambos dataframes a unir, el ejemplo muestra la manera de como unirlos sin que carguen ambos campos en el dataframe resultado, si lo que deseemos es que se incluyan, se ha de explicitar en el código.

   ```python
   # completar los datos del dataframe con la región
   df_whr_c = df_whr_all.join(df_countries,['country'],"left")
   ```

   Hasta ahora no he indicado como buena práctica la **observación de los datos**  que vamos leyendo, pero se ha de ir haciendo, utilizando instrucciones como el `describe`, el `info`  o el `show` sobre el dataframe. Pero es importante saber los datos que contiene, su calidad y saber por ejemplo si **existen nulos**, nos evitará errores en cálculos como el promedio.

   ```python
   print(f'nulos en gdp: {df_whr_c.filter(df_whr_c.gdp.isNull()).count()}')
   ```

5. Ahora busquemos las respuestas. Hagamos **preguntas/consultas**

   Ahí varias maneras de hacer consultas en la librería spark.sql, una es utilizando el, objeto **Window**. Este método es con el que he **implementado en Scala**, pues me parecía un reto mayor, no conocer el lenguaje y por tanto que fuese sin el uso de SQL.  

   ```python
   from pyspark.sql import Window
   w = Window.orderBy(F.col("ladder").desc())
   df_res_1= df_whr_c.filter(F.col("year") == 2021).withColumn("drank", F.rank().over(w)).filter(F.col("drank") == 1)
   ```

   O directamente **inyectando el la sentencia SQL**, en la que primero tendremos siempre que crear el objeto vista sobre el dataframe y luego pasar la sentencia.

   ```python
   # creando la vista temporal
   df_whr_c.createOrReplaceTempView("temp_whrAll")
   Qselect = "select country, ladder from " +\
        " (select *, row_number() OVER (ORDER BY ladder DESC) as rn " +\
        " FROM temp_whrAll WHERE year = 2021) tmp where rn = 1"
   df_res_1= spark.sql(Qselect)
   print(f"El país más feliz del 2021 es: { df_res_1.select('country').first().country} ")
   ```

   Pues ya tenemos el primer problema resulto y por tanto en el divide y vencerás, ya tenemos la primera victoria.

   Para el resto de preguntas, básicamente la forma es la misma, añadiendo algo de complejidad a la hora de extraer los valores concretos de filas o de un campo en concreto 

   a. Unir todos los datos de una columna en una list

   ```python
   countrys=df_res_3.select(df_res_3.country).rdd.flatMap(lambda x: x).collect()
   ```

   b. Extraer filas del dataframe y leer una de ellas (la primera en este caso) y un valor de una de sus columnas

   ```python
   row_list = df_res_4.collect()
   row_list[0].__getitem__("country")
   ```

#### Las preguntas a responder son:

1. ¿Cuál es el país más "feliz"del 2021 según la data? (considerar que la columna "Ladder score" mayor número más feliz es el país)

   *El país más feliz del 2021 es: Finland* 

2. ¿Cuál es el país más "feliz" del 2021 por continente según la data?

| region        | country     | ladder |
| ------------- | ----------- | ------ |
| Africa        | Mauritius   | 6.049  |
| Asia          | Israel      | 7.157  |
| Europe        | Finland     | 7.842  |
| North America | Canada      | 7.103  |
| Oceania       | New Zealand | 7.277  |
| South America | Uruguay     | 6.431  |

3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

   *Los paises que mas han ocupado el primer lugar son: Finland, Denmark*

4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

   *El Pais con mejor ranking GDP del 2020 es Ireland y ocupa la posición 15 del ranking de paises más felices del 2021*

5. ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?

   *El el promedio de gdp anual disminuyó en un:  3.38 %*

6. ¿Cuál es el país con mayor expectativa de vida ("Healthy life expectancy at birth")? Y ¿Cuánto tenia en ese indicador en el 2019? 

   *El Pais  con mayor expectativa de vida en 2021 es Iceland y en 2019 su esperanza de vida era de 0.983*


### Conclusión y enlaces al resultado

***Sin sacrificio no hay victoria*** decían en Transformers, y la satisfacción de enfrentarse al reto del Big Data, el Machine Learning y la Inteligencia Artificial obteniendo, por ahora, victorias en las batallas de los módulos pasados, encorajina para seguir hacia el final.

Al respecto del processing y Spark, remarcar cuantas posibilidades de análisis y productividad en ellos se ven con la aplicación de estas técnicas y conocimientos.

A seguir!!!

Te dejo en github el proyecto completo, el enlace a los 3 notebooks de la solución.

[BIG DATA PROCESSING en GitHub](https://github.com/emilio1310Git/big_data_processing)

Python de ejecución en Local, Python para Colab, Scala para Databricks y los datasets necesarios para que puedas tu practicar y ayudarme a mejorar con tus comentarios.

Gracias por tu tiempo!!!



