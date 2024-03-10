// Databricks notebook source
// No es necesarion en databriks inicializart sesion
// import org.apache.spark.sql.SparkSession

// Create SparkSession
// val spark:SparkSession = SparkSession.builder()
//     .master("local[1]").appName("SparkByExamples.com")
//     .getOrCreate()

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC #### Lectura de ficheros de datos

// COMMAND ----------

// world_happiness_report

val schema_whr = StructType(Array(
  StructField("Country name", StringType, true),
  StructField("year", IntegerType, true),
  StructField("Life Ladder", DoubleType , true),
  StructField("Log GDP per capita", DoubleType , true),
  StructField("Social support", DoubleType , true),
  StructField("Healthy life expectancy at birth", DoubleType , true)
))
val df_whr_i = spark.read.schema(schema_whr).option("header", "true").csv("/FileStore/big-data-processing-nov-2023/Datasets/world_happiness_report.csv")

// COMMAND ----------

// world_happiness_report_2021

val schema_whr21 = StructType(Array(
  StructField("Country name", StringType, true),
  StructField("Regional indicator", StringType, true),
  StructField("Ladder score", DoubleType , true),
  StructField("Standard error of ladder score", DoubleType , true),
  StructField("upperwhisker", DoubleType , true),
  StructField("lowerwhisker", DoubleType , true),
  StructField("Logged GDP per capita", DoubleType , true),
  StructField("Healthy life expectancy", DoubleType , true)
))
val df_whr21_i = spark.read.schema(schema_whr21).option("header", "true").csv("/FileStore/big-data-processing-nov-2023/Datasets/world_happiness_report_2021.csv")

// COMMAND ----------

// countries continents
val schema_countries = StructType(Array(
  StructField("country", StringType, true),
  StructField("region", StringType, true)
))
val df_countries = spark.read.schema(schema_countries).option("header", "true").csv("/FileStore/big-data-processing-nov-2023/Datasets/list_of_countries_by_continent_2024.csv")
display(df_countries.head(2))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Limpìeza y estandarización de datos
// MAGIC 1. Renombrar columnas
// MAGIC 2. Añadir el año de los datos del 2021
// MAGIC 3. Union de datos, todos los años con el año 2021
// MAGIC 4. Join de todos los datos anuales con sus regiones(continentes)
// MAGIC 5. Comprobar que columnas contiene nulos
// MAGIC

// COMMAND ----------

// world_happiness_report
val columnNames = Seq("Country name","Life Ladder","Log GDP per capita","Healthy life expectancy at birth","year")
val df_whr_1 = df_whr_i.select(columnNames.head, columnNames.tail: _*)
val newColumnNames = Seq("country", "ladder","gdp","healthy", "year")
val df_whr_2 = df_whr_1.toDF(newColumnNames:_*)

display(df_whr_2.head(2))

// COMMAND ----------

// world_happiness_report_2021
val columnNames21 = Seq("Country name","Ladder score","Logged GDP per capita","Healthy life expectancy")
val df_whr21_1 = df_whr21_i.select(columnNames21.head, columnNames21.tail: _*)
val newColumnNames21 = Seq("country", "ladder","gdp","healthy")
val df_whr21_2 = df_whr21_1.toDF(newColumnNames21:_*)
//  Añadir el año de los datos en una columna
val df_whr21_3 = df_whr21_2.withColumn("year", lit(2021))
display(df_whr21_3.head(2))

// COMMAND ----------

// Unir los dataframes ahora que tienen la misma estructura y tendremos todos los datos de todos los años
val df_whr_all = df_whr_2.union(df_whr21_3)
display(df_whr_all.head(2))


// COMMAND ----------

// completar los datos del dataframe con la región

val df_whr_c = df_whr_all.join(df_countries,"country","left")
display(df_whr_c.head(2))


// COMMAND ----------

// Observar los datos si contienen nulos, para eviar errores posteriores
println("nulos en Region: " + df_whr_c.filter(col("region").isNull).count())
println("nulos en ladder: " + df_whr_c.filter(col("ladder").isNull).count())
println("nulos en gdp: " + df_whr_c.filter(col("gdp").isNull).count())
println("nulos en healthy: " + df_whr_c.filter(col("healthy").isNull).count())
println("nulos en year: " + df_whr_c.filter(col("year").isNull).count())


// COMMAND ----------

// MAGIC %md
// MAGIC #### Soluciones

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// MAGIC %md
// MAGIC 1. **¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score” mayor número más feliz es el país)**

// COMMAND ----------

val windowYearLadder = Window.partitionBy("year").orderBy(desc("ladder"))
val rankYearLader = df_whr_c.withColumn("rn",row_number().over(windowYearLadder))
val row: Row = rankYearLader.filter(col("rn")===1 && col("year")===2021).head()
val mas_feliz: String = row.getAs[String]("country")
println("El país más feliz del 2021 es: " + mas_feliz)

// COMMAND ----------

// MAGIC %md
// MAGIC 2. **¿Cuál es el país más “feliz” del 2021 por continente según la data?**

// COMMAND ----------

val windowYearRegionLadder = Window.partitionBy("region","year").orderBy(desc("ladder"))
val rankYearLaderRegion= df_whr_c.withColumn("rn",rank().over(windowYearRegionLadder))
display(rankYearLaderRegion.filter(col("rn")===1 && col("year")===2021).select("region","country","ladder"))


// COMMAND ----------

// MAGIC %md
// MAGIC 3. **¿Cuál es el país que más veces ocupó el primer lugar en todos los años?**

// COMMAND ----------

val row: Row = rankYearLader.filter(col("rn")===1 && col("year")===2021).head()
val df_ranking_nveces =  rankYearLader.filter(col("rn")===1).select("country","year","ladder").groupBy("country").agg(count("country").as("repe")).orderBy(desc("repe"))
val row_repe: Row = df_ranking_nveces.head()
val max_repe: Integer = row_repe.getAs[Long]("repe").toInt
val df_r_countries =  df_ranking_nveces.filter(col("repe")===max_repe)
val countries=df_r_countries.select("country").map(f=>f.getString(0)).collect.toList
println("Los paises que mas han ocupado el primer lugar son: " + countries.mkString(","))


// COMMAND ----------

// MAGIC %md
// MAGIC 4. **¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?**

// COMMAND ----------

val windowYearGdp = Window.partitionBy("year").orderBy(desc("gdp"))
val rankYearGdp = df_whr_c.withColumn("rn",row_number().over(windowYearGdp))
val row_gdp_20: Row = rankYearGdp.filter(col("rn")===1 && col("year")===2020).head()
val mas_gdp_20: String = row_gdp_20.getAs[String]("country")
val df_posiciones= rankYearLader.filter(col("country")===mas_gdp_20 && col("year")===2021).select("rn")
val row_pos_gdp_20: Row = df_posiciones.head()
println("El Pais con mejor ranking GDP del 2020 es " + mas_gdp_20 + " y ocupa la posición " + row_pos_gdp_20.mkString(",") +" del ranking de paises más felices del 2021")


// COMMAND ----------

// MAGIC %md
// MAGIC 5. **¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?**

// COMMAND ----------

val df_ranking_nveces =  rankYearGdp.filter((col("year")===2020) || (col("year")===2021)).select("year","gdp").groupBy("year").agg(avg("gdp").as("promedio_gdp"))
var promedio2021 =df_ranking_nveces.filter(col("year")===2021).head().getAs[Double]("promedio_gdp")
var promedio2020 =df_ranking_nveces.filter(col("year")===2020).head().getAs[Double]("promedio_gdp")
val diferencia = Math.abs(((promedio2021- promedio2020)/promedio2021)*100)
if (promedio2021 > promedio2020){
  println("El el promedio de gdp anual aumentó en un: "+  "%.2f".format(diferencia) + "%")
}
else{
  println("El el promedio de gdp anual disminuyó en un: "+ "%.2f".format(diferencia) + "%")
}

// COMMAND ----------

// MAGIC %md
// MAGIC 6. **¿Cuál es el país con mayor expectativa de vide (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia en ese indicador en el 2019?**

// COMMAND ----------

val windowYearHealthy = Window.partitionBy("year").orderBy(desc("Healthy"))
val rankYearHealthy = df_whr_c.withColumn("rn",row_number().over(windowYearHealthy))
val row_Healthy_21: Row = rankYearHealthy.filter(col("rn")===1 && col("year")===2021).head()
val mas_Healthy_21: String = row_Healthy_21.getAs[String]("country")
val df_healty_19= rankYearLader.filter(col("country")===mas_Healthy_21 && col("year")===2019).select("Healthy")
val row_df_healty_19: Row = df_healty_19.head()

println("El Pais con mejor expectativa de vida del 2021 es " + mas_Healthy_21 + " y su expectativa en 2019 era: " + row_df_healty_19.mkString(","))
