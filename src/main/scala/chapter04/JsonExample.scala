package chapter04

import org.apache.spark.sql.SparkSession

object JsonExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-JSON")
      .master("local[*]")
      .getOrCreate()

    println("=== JSON DATA SOURCE ===")

    // PASO 1: Leer CSV original
    val csvPath = "data/chapter04/departuredelays.csv"

    val flightsDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)

    println("\nDatos originales (CSV):")
    flightsDF.show(5)

    // PASO 2: Escribir como JSON
    val jsonOutputPath = "data/chapter04/output/json"

    flightsDF.write
      .format("json")
      .mode("overwrite")
      .save(jsonOutputPath)

    println(s"\n✓ Datos guardados como JSON en: $jsonOutputPath")

    // PASO 3: Leer JSON como DataFrame
    val jsonDF = spark.read
      .format("json")
      .load(jsonOutputPath)

    println("\nDatos leídos desde JSON:")
    jsonDF.show(5)

    // PASO 4: Crear vista temporal y consultar con SQL
    jsonDF.createOrReplaceTempView("us_delay_flights_json")

    println("\nConsulta SQL sobre JSON:")
    spark.sql("""
      SELECT origin, destination, delay
      FROM us_delay_flights_json
      WHERE delay > 90
      ORDER BY delay DESC
    """).show(5)

    println("✓ JSON leído, escrito y consultado con SQL")

    spark.stop()
  }
}
