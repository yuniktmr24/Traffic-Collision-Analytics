package store

import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.TrafficCollisionSchema

object CrashAggregateDataframe {
  val spark = SparkSession.builder()
    .appName("Crash Data Analytics - 2016 to 2023")
    .master("local[*]") // Use local mode with all cores
    .getOrCreate()

  def loadDataFrame(csvFilePath: String): DataFrame = {
    var df = spark.read
      .option("header", "true")
      .schema(TrafficCollisionSchema.schema)
      .csv(csvFilePath)

    // Rename columns to avoid issues with parentheses in SQL queries
    df = df.withColumnRenamed("Distance(mi)", "Distance_mi")
      .withColumnRenamed("Temperature(F)", "Temperature")
      .withColumnRenamed("Wind_Chill(F)", "Wind_Chill")
      .withColumnRenamed("Humidity(%)", "Humidity_Percent")
      .withColumnRenamed("Pressure(in)", "Pressure_in")
      .withColumnRenamed("Visibility(mi)", "Visibility_mi")
      .withColumnRenamed("Wind_Speed(mph)", "Wind_Speed")
      .withColumnRenamed("Precipitation(in)", "Precipitation")

    df
  }
}