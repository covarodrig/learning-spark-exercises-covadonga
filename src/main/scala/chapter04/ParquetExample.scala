package chapter04

import org.apache.spark.sql.SparkSession

object ParquetExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-Parquet")
      .master("local[*]")
      .getOrCreate()

    println("=== PARQUET DATA SOURCE ===")

    // PASO 1: Leer Parquet como DataFrame
    val parquetPath = "data/chapter04/output/parquet"

    val parquetDF = spark.read
      .format("parquet")
      .load(parquetPath)

    println("\nMostrando datos desde Parquet:")
    parquetDF.show(5)

    // PASO 2: Crear vista temporal desde Parquet (SQL)
    parquetDF.createOrReplaceTempView("us_delay_flights_parquet")

    // PASO 3: Consultar con SQL
    println("\nConsulta SQL sobre Parquet:")
    spark.sql("""
      SELECT origin, destination, delay
      FROM us_delay_flights_parquet
      WHERE delay > 60
      ORDER BY delay DESC
    """).show(5)

    println("✓ Parquet leído y consultado con SQL")

    spark.stop()
  }
}
