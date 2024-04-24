package store

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import schema.TrafficCollisionSchema
import store.CrashAggregateDataframe.spark

object CrashYearlyDataframe {
  val spark = SparkSession.builder()
    .appName("Yearly Crash Data Analytics")
//    .config("spark.io.compression.codec", "lz4")
    .config("spark.sql.parquet.compression.codec", "gzip")
    .master("local[*]") // Use local mode with all cores
    .getOrCreate()

  def loadDataFrame(csvFilePath: String): DataFrame = {
    var df = CrashAggregateDataframe.loadDataFrame(csvFilePath)
      .withColumn("Year", year(col("Start_Time")))
    df
  }

}

