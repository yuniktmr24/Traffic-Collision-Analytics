package store

import org.apache.spark.sql.SparkSession
import schema.TrafficCollisionSchema

object CrashAggregateDataframe {
  val spark = SparkSession.builder()
    .appName("Crash Data Analytics - 2016 to 2023")
    .master("local[*]") // Use local mode with all cores
    .getOrCreate()

  // Read data using the defined schema
  val df = spark.read
    .option("header", "true") // Assumes the first row is a header
    .schema(TrafficCollisionSchema.schema)
    .csv("src/main/resources/US_Accidents_March23.csv")
}
