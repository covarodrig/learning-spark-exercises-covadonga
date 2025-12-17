package chapter04

import org.apache.spark.sql.SparkSession

object SparkSQLBasics {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Basics - Chapter 4")
      .master("local[*]")
      .getOrCreate()

    println("SparkSession creada correctamente")

    spark.stop()
  }
}
