package chapter04

import org.apache.spark.sql.SparkSession

/**
 * Chapter 4 - Binary Files Data Source
 * Pages 109-110
 */
object BinaryFilesExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-BinaryFiles")
      .master("local[*]")
      .getOrCreate()

    println("=== BINARY FILES DATA SOURCE ===")

    // Ruta a las imágenes (adaptada a tu proyecto)
    val path = "data/chapter04/train_images"

    println(s"Leyendo archivos binarios desde: $path")

    val binaryFilesDF = spark.read
      .format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .load(path)

    println("\nSchema del DataFrame:")
    binaryFilesDF.printSchema()

    println("\nPrimeros archivos leídos:")
    binaryFilesDF
      .select("path", "length", "modificationTime")
      .show(5, truncate = false)

    println("\nLeyendo archivos IGNORANDO particiones (recursiveFileLookup = true)")

    val binaryFilesRecursiveDF = spark.read
      .format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .option("recursiveFileLookup", "true")
      .load(path)

    binaryFilesRecursiveDF
      .select("path", "length")
      .show(5, truncate = false)

    spark.stop()
  }
}
