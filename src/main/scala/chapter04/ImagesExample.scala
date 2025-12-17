package chapter04

import org.apache.spark.sql.SparkSession

/**
 * Chapter 4 - Images Data Source
 * Pages 108-109 (Learning Spark 2nd Ed.)
 */
object ImagesExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-Images")
      .master("local[*]")
      .config("spark.hadoop.io.native.lib.available", "false")
      .getOrCreate()


    // ====================================================
    // IMAGES DATA SOURCE (Hive-style partitioned)
    // ====================================================
    val imageDir = "data/chapter04/train_images"

    println("=== IMAGES DATA SOURCE ===")
    println(s"Ruta de imágenes: $imageDir\n")

    val imagesDF = spark.read
      .format("image")
      .load(imageDir)

    // Schema (igual que el libro)
    println("Schema:")
    imagesDF.printSchema()

    // Selección de columnas EXACTA del libro + PATH
    println("\nDatos de imágenes:")
    imagesDF.select(
      "image.origin",     // PATH COMPLETO
      "image.height",
      "image.width",
      "image.nChannels",
      "image.mode",
      "label"             // viene del folder label=0 / label=1
    ).show(10, false)

    spark.stop()
  }
}
