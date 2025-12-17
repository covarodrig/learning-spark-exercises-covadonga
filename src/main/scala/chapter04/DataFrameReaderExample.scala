package chapter04

import org.apache.spark.sql.SparkSession

object DataFrameReaderExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Chapter04-DataFrameReader")
      .master("local[*]")
      .getOrCreate()

    println("=== DataFrameReader examples ===")

    // CSV
    val csvDF = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("data/chapter04/departuredelays.csv")

    csvDF.show(5)
    csvDF.printSchema()

    spark.stop()
  }
}
