package chapter04

import org.apache.spark.sql.SparkSession

object CsvExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-CSV")
      .master("local[*]")
      .getOrCreate()

    println("=== CSV DATA SOURCE ===")

    // PASO 1: Definir schema explícito (como en el libro)
    val schema = """
      date STRING,
      delay INT,
      distance INT,
      origin STRING,
      destination STRING
    """

    val csvPath = "data/chapter04/departuredelays.csv"

    // PASO 2: Leer CSV con opciones
    val flightsDF = spark.read
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .option("mode", "FAILFAST")
      .load(csvPath)

    println("\nDatos leídos desde CSV:")
    flightsDF.show(5)

    // PASO 3: Crear vista temporal SQL
    flightsDF.createOrReplaceTempView("us_delay_flights_csv")

    println("\nConsulta SQL sobre CSV:")
    spark.sql("""
      SELECT origin, destination, delay
      FROM us_delay_flights_csv
      WHERE delay > 60
      ORDER BY delay DESC
    """).show(5)

    // PASO 4: Guardar CSV (DataFrameWriter)
    val outputPath = "data/chapter04/output/csv"

    flightsDF.write
      .format("csv")
      .mode("overwrite")
      .save(outputPath)

    println(s"\n✓ CSV guardado en: $outputPath")

    spark.stop()
  }
}
