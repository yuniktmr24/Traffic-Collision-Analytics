package store

import org.apache.spark.sql.SparkSession
import schema.NYCCollisionSchema

object CrashNYCDataFrame {
  val spark = SparkSession.builder()
    .appName("NYC Crash Analytics")
//    .master("local[*]") // Use local mode with all cores
    .getOrCreate()

  // Read data using the defined schema
//  var df = spark.read
//    .option("header", "true") // Assumes the first row is a header
//    .schema(NYCCollisionSchema.schema)
//    .csv("src/main/resources/NYC_Motor_Vehicle_Collisions_-_Crashes.csv")

}
