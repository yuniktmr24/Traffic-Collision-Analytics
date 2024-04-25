package store

import org.apache.spark.sql.SparkSession
import schema.ChicagoCollisionSchema

object CrashChicagoDataFrame {
  val spark = SparkSession.builder()
    .appName("Chicago Crash Analytics")
//     .master("local[*]") // Use local mode with all cores
    .getOrCreate()

  // Read data using the defined schema
  // var df = spark.read
  //   .option("header", "true") // Assumes the first row is a header
  //   .schema(ChicagoCollisionSchema.schema)
  //   .csv("src/main/resources/Chicago_Traffic_Crashes.csv")

}
