package chapter04

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Chapter 4 - Basic SQL Queries on Flight Delays
 * Pages 85-89
 * 
 * Conceptos:
 * - Crear vistas temporales
 * - Usar spark.sql() para queries
 * - Interoperabilidad SQL y DataFrame API
 */
object FlightDelays {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Chapter04-FlightDelays")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("=== Capítulo 4: Spark SQL y DataFrames ===\n")

    // Ruta al archivo CSV
    val csvFile = "data/chapter04/departuredelays.csv"

    // PASO 1: Leer CSV y crear DataFrame
    println("PASO 1: Leyendo datos de vuelos...")
    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    println("Schema del DataFrame:")
    df.printSchema()
    
    println("\nPrimeras filas:")
    df.show(5)

    // PASO 2: Crear vista temporal
    println("\nPASO 2: Creando vista temporal 'us_delay_flights_tbl'...")
    df.createOrReplaceTempView("us_delay_flights_tbl")
    println("✓ Vista temporal creada")

    // PASO 3: Query 1 - Vuelos con distancia > 1000 millas (Página 86)
    println("\n" + "="*60)
    println("QUERY 1: Vuelos con distancia mayor a 1000 millas")
    println("="*60)
    
    spark.sql("""
      SELECT distance, origin, destination 
      FROM us_delay_flights_tbl 
      WHERE distance > 1000 
      ORDER BY distance DESC
    """).show(10)

    // PASO 4: Query 2 - Vuelos SFO a ORD con delay > 120 min (Página 87)
    println("\n" + "="*60)
    println("QUERY 2: Vuelos de San Francisco (SFO) a Chicago (ORD)")
    println("         con retraso mayor a 2 horas")
    println("="*60)
    
    spark.sql("""
      SELECT date, delay, origin, destination 
      FROM us_delay_flights_tbl 
      WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD' 
      ORDER BY delay DESC
    """).show(10)

    // PASO 5: Query 3 - Clasificar retrasos con CASE (Página 87-88)
    println("\n" + "="*60)
    println("QUERY 3: Clasificando retrasos en categorías")
    println("="*60)
    
    spark.sql("""
      SELECT delay, origin, destination,
        CASE
          WHEN delay > 360 THEN 'Very Long Delays'
          WHEN delay > 120 AND delay <= 360 THEN 'Long Delays'
          WHEN delay > 60 AND delay <= 120 THEN 'Short Delays'
          WHEN delay > 0 AND delay <= 60 THEN 'Tolerable Delays'
          WHEN delay = 0 THEN 'No Delays'
          ELSE 'Early'
        END AS Flight_Delays
      FROM us_delay_flights_tbl
      ORDER BY origin, delay DESC
    """).show(10)

    // PASO 6: Misma query pero con DataFrame API (Página 88)
    println("\n" + "="*60)
    println("COMPARACIÓN: Misma query con DataFrame API")
    println("="*60)
    
    df.select("distance", "origin", "destination")
      .where($"distance" > 1000)
      .orderBy(desc("distance"))
      .show(10)

    println("\n✓ Todas las queries ejecutadas correctamente")
    println("\nCONCEPTO CLAVE: SQL y DataFrame API producen los mismos resultados")
    println("porque ambos usan el mismo motor de ejecución (Catalyst)")

    spark.stop()
  }
}