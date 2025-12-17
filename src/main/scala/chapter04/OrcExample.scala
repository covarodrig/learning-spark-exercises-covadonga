package chapter04

import org.apache.spark.sql.SparkSession

object OrcExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-ORC")
      .master("local[*]")
      .getOrCreate()

    println("=== ORC DATA SOURCE ===")

    // PASO 1: Leer ORC
    val orcPath = "data/chapter04/orc"

    val orcDF = spark.read
      .format("orc")
      .load(orcPath)

    println("\nDatos leídos desde ORC:")
    orcDF.show(5, false)

    // PASO 2: Crear vista temporal
    orcDF.createOrReplaceTempView("us_delay_flights_orc")

    println("\nConsulta SQL sobre ORC:")
    spark.sql("""
      SELECT origin, destination, delay
      FROM us_delay_flights_orc
      WHERE delay > 120
      ORDER BY delay DESC
    """).show(5, false)

    // PASO 3: Guardar como ORC
    val outputPath = "data/chapter04/output/orc"

    orcDF.write
      .format("orc")
      .mode("overwrite")
      .option("compression", "snappy")
      .save(outputPath)

    println(s"\n✓ ORC guardado en: $outputPath")

    spark.stop()
  }
}
