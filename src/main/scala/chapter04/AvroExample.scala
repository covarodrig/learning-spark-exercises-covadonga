package chapter04

import org.apache.spark.sql.SparkSession

object AvroExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-Avro")
      .master("local[*]")
      .getOrCreate()

    println("=== AVRO DATA SOURCE ===")

    // PASO 1: Leer Avro
    val avroPath = "data/chapter04/avro"

    val avroDF = spark.read
      .format("avro")
      .load(avroPath)

    println("\nDatos leídos desde Avro:")
    avroDF.show(5, false)

    // PASO 2: Crear vista temporal
    avroDF.createOrReplaceTempView("us_delay_flights_avro")

    println("\nConsulta SQL sobre Avro:")
    spark.sql("""
      SELECT origin, destination, delay
      FROM us_delay_flights_avro
      WHERE delay > 90
      ORDER BY delay DESC
    """).show(5, false)

    // PASO 3: Guardar como Avro
    val outputPath = "data/chapter04/output/avro"

    avroDF.write
      .format("avro")
      .mode("overwrite")
      .save(outputPath)

    println(s"\n✓ Avro guardado en: $outputPath")

    spark.stop()
  }
}
