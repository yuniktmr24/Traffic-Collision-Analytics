package store

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CrashYearlyDataframe {
  val spark = SparkSession.builder()
    .appName("Yearly Crash Data Analytics")
//    .config("spark.io.compression.codec", "lz4")
    .config("spark.sql.parquet.compression.codec", "gzip")
    .master("local[*]") // Use local mode with all cores
    .getOrCreate()

  val df = CrashAggregateDataframe.df
    .withColumn("Year", year(col("Start_Time")))
}

