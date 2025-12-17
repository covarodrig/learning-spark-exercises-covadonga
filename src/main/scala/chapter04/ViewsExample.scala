package chapter04

import org.apache.spark.sql.SparkSession

/**
 * Chapter 4 - Temporary and Global Temporary Views
 * Pages 91-92
 * 
 * Conceptos:
 * - Vistas temporales (por SparkSession)
 * - Vistas globales (compartidas entre SparkSessions)
 */
object ViewsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Chapter04-Views")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("=== VISTAS TEMPORALES Y GLOBALES ===\n")

    // Preparar datos
    val csvFile = "data/chapter04/departuredelays.csv"
    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")

    // PASO 1: Crear vista temporal normal (Página 91)
    println("PASO 1: Creando vista temporal (solo para esta SparkSession)")
    
    val dfJFK = spark.sql("""
      SELECT date, delay, origin, destination 
      FROM us_delay_flights_tbl 
      WHERE origin = 'JFK'
    """)
    
    dfJFK.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")
    println("✓ Vista temporal creada: us_origin_airport_JFK_tmp_view")

    // PASO 2: Crear vista global temporal (Página 91)
    println("\nPASO 2: Creando vista GLOBAL temporal (compartida)")
    
    val dfSFO = spark.sql("""
      SELECT date, delay, origin, destination 
      FROM us_delay_flights_tbl 
      WHERE origin = 'SFO'
    """)
    
    dfSFO.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    println("✓ Vista GLOBAL creada: us_origin_airport_SFO_global_tmp_view")

    // PASO 3: Consultar vista temporal (Página 92)
    println("\n" + "="*60)
    println("Consultando vista TEMPORAL (sin prefijo):")
    println("="*60)
    spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show(5)

    // PASO 4: Consultar vista global (Página 92)
    println("\n" + "="*60)
    println("Consultando vista GLOBAL (con prefijo global_temp):")
    println("="*60)
    spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view").show(5)

    // PASO 5: Demostrar diferencia con otra SparkSession
    println("\n" + "="*60)
    println("DEMOSTRACIÓN: Diferencia entre vistas")
    println("="*60)
    
    println("\nCreando nueva SparkSession...")
    val spark2 = spark.newSession()
    
    println("\nIntentando acceder a vista TEMPORAL desde spark2:")
    try {
      spark2.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show(1)
      println("✓ Acceso exitoso (no debería pasar)")
    } catch {
      case e: Exception => 
        println("❌ ERROR: Vista temporal NO visible desde otra SparkSession")
        println(s"   ${e.getMessage}")
    }
    
    println("\nIntentando acceder a vista GLOBAL desde spark2:")
    try {
      spark2.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view").show(2)
      println("✓ Vista GLOBAL accesible desde cualquier SparkSession")
    } catch {
      case e: Exception => 
        println(s"❌ Error: ${e.getMessage}")
    }

    // PASO 6: Eliminar vistas (Página 92)
    println("\n" + "="*60)
    println("PASO 6: Eliminando vistas")
    println("="*60)
    
    spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")
    println("✓ Vista temporal eliminada")
    
    spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    println("✓ Vista global eliminada")

    println("\n" + "="*60)
    println("RESUMEN:")
    println("="*60)
    println("Vista TEMPORAL:")
    println("  - Solo visible en su SparkSession")
    println("  - Acceso: SELECT * FROM vista_nombre")
    println("\nVista GLOBAL:")
    println("  - Visible en todas las SparkSessions")
    println("  - Acceso: SELECT * FROM global_temp.vista_nombre")

    spark.stop()
  }
}