package driver

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import store.CrashYearlyDataframe

import scala.collection.mutable

object CrashYearly {
  def main(args: Array[String]): Unit = {
    val spark = CrashYearlyDataframe.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val df = CrashYearlyDataframe.df

    // Find distinct years in the DataFrame
    val years = df.select("Year").distinct().collect().map(_.getInt(0))

    // Initialize a mutable map to hold DataFrames for each year
    val yearlyDataFrames = mutable.Map[Int, org.apache.spark.sql.DataFrame]()

    // Loop through each year and filter DataFrame by that year
    years.foreach { year =>
      val yearlyDataFrame = df.filter(col("Year") === year)
      yearlyDataFrames(year) = yearlyDataFrame
      println(s"Data for $year has ${yearlyDataFrame.count()} rows.")
    }

    // Example usage: show data for a specific year, e.g., 2019
    //yearlyDataFrames.get(2019).foreach(_.show())

    spark.stop()


  }
}
