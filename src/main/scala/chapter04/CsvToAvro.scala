package chapter04

import org.apache.spark.sql.SparkSession

object CsvToAvro {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CSV to Avro")
      .master("local[*]")
      .getOrCreate()

    val csvPath = "data/chapter04/departuredelays.csv"
    val avroPath = "data/chapter04/avro"

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    df.write
      .format("avro")
      .mode("overwrite")
      .save(avroPath)

    println("✓ Avro creado desde CSV")

    spark.stop()
  }
}
