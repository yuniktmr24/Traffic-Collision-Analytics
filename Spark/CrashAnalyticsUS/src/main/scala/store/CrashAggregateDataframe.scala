package store

import org.apache.spark.sql.SparkSession
import schema.TrafficCollisionSchema

object CrashAggregateDataframe {
  val spark = SparkSession.builder()
    .appName("Crash Data Analytics - 2016 to 2023")
    .master("local[*]") // Use local mode with all cores
    .getOrCreate()

  // Read data using the defined schema
  var df = spark.read
    .option("header", "true") // Assumes the first row is a header
    .schema(TrafficCollisionSchema.schema)
    .csv("src/main/resources/US_Accidents_March23.csv")

  //dropping the parentheses. to avoid spark sql analysisException
  df = df.withColumnRenamed("Distance(mi)", "Distance_mi")
  df = df.withColumnRenamed("Temperature(F)", "Temperature")
  df = df.withColumnRenamed("Wind_Chill(F)", "Wind_Chill")
  df = df.withColumnRenamed("Humidity(%)", "Humidity_Percent")
  df = df.withColumnRenamed("Pressure(in)", "Pressure_in")
  df = df.withColumnRenamed("Visibility(mi)", "Visibility_mi")
  df = df.withColumnRenamed("Wind_Speed(mph)", "Wind_Speed")
  df = df.withColumnRenamed("Precipitation(in)", "Precipitation")

  //could persist this as parquet for caching. but maybe not worth the storage overhead
}