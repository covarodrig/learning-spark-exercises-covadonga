package chapter03

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Chapter 3 - Dataset API with IoT Devices
 * Pages 69-74
 */

// Case class para el JSON original
case class DeviceIoTData(
  battery_level: Long,
  c02_level: Long,
  cca2: String,
  cca3: String,
  cn: String,
  device_id: Long,
  device_name: String,
  humidity: Long,
  ip: String,
  latitude: Double,
  lcd: String,
  longitude: Double,
  scale: String,
  temp: Long,
  timestamp: Long
)

// Case class DEBE ir FUERA del object
case class DeviceTempByCountry(
  temp: Long,
  device_name: String,
  device_id: Long,
  cca3: String
)

object IoTDevices {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("IoTDevices")
      .master("local[*]")
      .getOrCreate()

    // NECESARIO para Encoders
    import spark.implicits._

    // Read JSON file and convert to Dataset
    val ds = spark.read
      .json("data/chapter03/iot_devices.json")
      .as[DeviceIoTData]

    println("=== IoT Devices Data ===")
    ds.show(5, truncate = false)

    // Filter with lambda function (Page 72)
    println("\n=== Devices with temp > 30 and humidity > 70 ===")
    val filterTempDS = ds.filter(d => d.temp > 30 && d.humidity > 70)
    filterTempDS.show(5, truncate = false)

    // Map and create new Dataset (Page 72)
    println("\n=== Device Temp by Country (temp > 25) ===")
    val dsTemp = ds
      .filter(d => d.temp > 25)
      .map(d => DeviceTempByCountry(d.temp, d.device_name, d.device_id, d.cca3))

    dsTemp.show(5, truncate = false)

    // Get first device
    println("\n=== First Device ===")
    val device = dsTemp.first()
    println(device)

    // Alternative query using select (Page 73)
    println("\n=== Alternative query with select ===")
    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .where($"temp" > 25)
      .as[DeviceTempByCountry]

    dsTemp2.show(5, truncate = false)

    spark.stop()
  }
}
