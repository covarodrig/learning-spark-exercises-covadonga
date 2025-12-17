package chapter04

import org.apache.spark.sql.SparkSession

/**
 * Chapter 4 - Managed vs Unmanaged Tables
 * Pages 89-93
 * 
 * Conceptos:
 * - Crear bases de datos
 * - Tablas Managed (Spark gestiona todo)
 * - Tablas Unmanaged (tú gestionas los datos)
 */
object ManagedUnmanagedTables {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Chapter04-Tables")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("=== TABLAS MANAGED VS UNMANAGED ===\n")

    // PASO 1: Crear base de datos (Página 90)
    println("PASO 1: Creando base de datos 'learn_spark_db'...")
    spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")
    println("✓ Base de datos creada y seleccionada\n")

    // Ver bases de datos disponibles
    println("Bases de datos disponibles:")
    spark.catalog.listDatabases().show(false)

    // PASO 2: Crear tabla MANAGED con SQL (Página 90)
    println("\n" + "="*60)
    println("PASO 2: Creando tabla MANAGED con SQL")
    println("="*60)
    
    spark.sql("""
      CREATE TABLE IF NOT EXISTS managed_us_delay_flights_tbl (
        date STRING, 
        delay INT, 
        distance INT, 
        origin STRING, 
        destination STRING
      )
    """)
    println("✓ Tabla MANAGED creada: managed_us_delay_flights_tbl")

    // PASO 3: Crear tabla MANAGED con DataFrame API (Página 90)
    println("\n" + "="*60)
    println("PASO 3: Creando tabla MANAGED con DataFrame API")
    println("="*60)
    
    val csvFile = "data/chapter04/departuredelays.csv"
    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    
    val flightsDF = spark.read
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .load(csvFile)
    
    flightsDF.write
      .mode("overwrite")
      .saveAsTable("managed_us_delay_flights_tbl")
    
    println("✓ Tabla MANAGED guardada con datos")

    // Verificar que la tabla tiene datos
    println("\nPrimeras filas de la tabla MANAGED:")
    spark.sql("SELECT * FROM managed_us_delay_flights_tbl LIMIT 5").show()

    // PASO 4: Crear tabla UNMANAGED con SQL (Página 91)
    println("\n" + "="*60)
    println("PASO 4: Creando tabla UNMANAGED con SQL")
    println("="*60)
    
    spark.sql("""
      CREATE TABLE IF NOT EXISTS us_delay_flights_tbl (
        date STRING, 
        delay INT, 
        distance INT, 
        origin STRING, 
        destination STRING
      ) 
      USING csv 
      OPTIONS (
        PATH 'data/chapter04/departuredelays.csv',
        header 'true'
      )
    """)
    println("✓ Tabla UNMANAGED creada (datos externos)")

    // PASO 5: Crear tabla UNMANAGED con DataFrame API (Página 91)
    println("\n" + "="*60)
    println("PASO 5: Creando tabla UNMANAGED con DataFrame API")
    println("="*60)
    
    flightsDF.write
      .mode("overwrite")
      .option("path", "data/chapter04/us_flights_delay_unmanaged")
      .saveAsTable("us_delay_flights_unmanaged_tbl")
    
    println("✓ Tabla UNMANAGED guardada (datos en path específico)")

    // PASO 6: Ver metadatos de las tablas (Página 93)
    println("\n" + "="*60)
    println("PASO 6: Explorando metadatos con Catalog")
    println("="*60)
    
    println("\nTablas en learn_spark_db:")
    spark.catalog.listTables("learn_spark_db").show(false)
    
    println("\nColumnas de managed_us_delay_flights_tbl:")
    spark.catalog.listColumns("managed_us_delay_flights_tbl").show(false)

    // PASO 7: Diferencias entre MANAGED y UNMANAGED
    println("\n" + "="*60)
    println("DIFERENCIAS CLAVE:")
    println("="*60)
    println("MANAGED TABLE:")
    println("  - Spark controla metadatos Y datos")
    println("  - DROP TABLE borra TODO")
    println("  - Datos en: spark-warehouse/")
    println("\nUNMANAGED TABLE:")
    println("  - Spark solo controla metadatos")
    println("  - DROP TABLE solo borra metadatos")
    println("  - Datos quedan en tu PATH especificado")

    // No cerramos Spark aquí para poder ver las tablas en spark-warehouse
    println("\n✓ Ejercicio completado")
    println("\nPuedes verificar las tablas en: spark-warehouse/learn_spark_db.db/")
    
    spark.stop()
  }
}