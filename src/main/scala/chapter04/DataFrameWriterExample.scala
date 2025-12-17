package chapter04

import org.apache.spark.sql.SparkSession

object DataFrameWriterExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-DataFrameWriter")
      .master("local[*]")
      .getOrCreate()

    println("=== DataFrameWriter examples ===")

    // Leer CSV
    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/chapter04/departuredelays.csv")

    df.show()

    // Guardar como Parquet (formato por defecto)
    df.write
      .mode("overwrite")
      .save("data/chapter04/output/parquet")

    // Guardar como JSON
    df.write
      .format("json")
      .mode("overwrite")
      .save("data/chapter04/output/json")

    // Guardar como CSV
    df.write
      .format("csv")
      .mode("overwrite")
      .option("header", "true")
      .save("data/chapter04/output/csv")

    println("Archivos escritos correctamente")

    spark.stop()
  }
}
