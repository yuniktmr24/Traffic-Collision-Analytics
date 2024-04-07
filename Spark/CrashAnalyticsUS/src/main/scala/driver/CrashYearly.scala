package driver

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import schema.TrafficCollisionSchema
import store.CrashYearlyDataframe

import scala.collection.mutable

object CrashYearly {
  def main(args: Array[String]): Unit = {
    val spark = CrashYearlyDataframe.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val df = CrashYearlyDataframe.df
    val schema = TrafficCollisionSchema.schema

    // Find distinct years in the DataFrame
    val years = df.select("Year").distinct().collect().map(_.getInt(0))

    // Initialize a mutable map to hold DataFrames for each year
    val yearlyDataFrames = mutable.Map[Int, org.apache.spark.sql.DataFrame]()

    // FileSystem setup for checking existence of Parquet files
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Loop through each year and process DataFrame by that year
    years.foreach { year =>
      val path = s"src/main/scala/cache/yearly/year_$year"
      if (fs.exists(new Path(path))) {
        println(s"Loading data for $year from cache.")
        //val yearlyDataFrame = spark.read.format("parquet").load(path)
        val yearlyDataFrame = spark.read.schema(schema).parquet(path)
        yearlyDataFrames(year) = yearlyDataFrame
        println(s"Data for $year has ${yearlyDataFrame.count()} rows.")
      } else {
        println(s"Data for $year not found in cache, processing and caching.")
        val yearlyDataFrame = df.filter(col("Year") === year)
        yearlyDataFrames(year) = yearlyDataFrame
        yearlyDataFrame.write.format("parquet").save(path)
        println(s"Data for $year has been written to $path")
      }
    }

    // Example usage: show data for a specific year, e.g., 2019
    //yearlyDataFrames.get(2019).foreach(_.show())

    spark.stop()


  }
}
