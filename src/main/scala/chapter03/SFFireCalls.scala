package chapter03

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Chapter 3 - SF Fire Department Calls Analysis
 * Pages 58-68
 */
object SFFireCalls {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SFFireCalls")
      .master("local[*]")
      .getOrCreate()

    // IMPORTANTE: Esto permite usar $ para columnas
    import spark.implicits._

    // Define schema programmatically
    val fireSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)))

    // Read the CSV file
    val sfFireFile = "data/chapter03/sf-fire-calls.csv"
    val fireDF = spark.read
      .schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    println("=== Sample Data ===")
    fireDF.show(5)

    // Projections and filters (Page 61)
    println("\n=== Projection and Filter: Non-Medical Incidents ===")
    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where($"CallType" =!= "Medical Incident")
    fewFireDF.show(5, false)

    // Count distinct call types (Page 61-62)
    println("\n=== Distinct Call Types ===")
    fireDF
      .select("CallType")
      .where($"CallType".isNotNull)
      .agg(countDistinct("CallType").as("DistinctCallTypes"))
      .show()

    // List distinct call types (Page 62)
    println("\n=== List of Distinct Call Types ===")
    fireDF
      .select("CallType")
      .where($"CallType".isNotNull)
      .distinct()
      .show(10, false)

    // Rename column and filter (Page 63)
    println("\n=== Response Delays > 5 minutes ===")
    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where($"ResponseDelayedinMins" > 5)
      .show(5, false)

    // Convert date strings to timestamps (Page 64)
    println("\n=== Converting dates to timestamps ===")
    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp($"CallDate", "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp($"WatchDate", "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp($"AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    // Extract years from dates (Page 65)
    println("\n=== Years covered in dataset ===")
    fireTsDF
      .select(year($"IncidentDate"))
      .distinct()
      .orderBy(year($"IncidentDate"))
      .show()

    // Aggregations: Most common call types (Page 66-67)
    println("\n=== Most Common Call Types ===")
    fireTsDF
      .select("CallType")
      .where($"CallType".isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    // Statistical operations (Page 67)
    println("\n=== Statistical Summary ===")
    fireTsDF
      .select(sum("NumAlarms"), avg("ResponseDelayedinMins"),
        min("ResponseDelayedinMins"), max("ResponseDelayedinMins"))
      .show()

    spark.stop()
  }
}