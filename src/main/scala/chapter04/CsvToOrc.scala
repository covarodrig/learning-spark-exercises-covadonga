package chapter04

import org.apache.spark.sql.SparkSession

object CsvToOrc {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CSV to ORC")
      .master("local[*]")
      .getOrCreate()

    val csvPath = "data/chapter04/departuredelays.csv"
    val orcPath = "data/chapter04/orc"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    df.write
      .format("orc")
      .mode("overwrite")
      .save(orcPath)

    println("✓ ORC creado desde CSV")

    spark.stop()
  }
}
