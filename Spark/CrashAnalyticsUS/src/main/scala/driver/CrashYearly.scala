package driver

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import query.aggregate.AggregateQueryRepository
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
      try {
        var yearlyDataFrame: DataFrame = null

        if (fs.exists(new Path(path))) {
          println(s"Loading data for $year from cache.")
          yearlyDataFrame = spark.read.schema(schema).parquet(path)
        } else {
          println(s"Data for $year not found in cache, processing and caching.")
          yearlyDataFrame = df.filter(col("Year") === year)
          yearlyDataFrame.write.format("parquet").save(path)
        }

        yearlyDataFrame.unpersist() // Ensure no old version is cached
        yearlyDataFrame = yearlyDataFrame
          .withColumnRenamed("Temperature(F)", "Temperature")
          .withColumnRenamed("Distance(mi)", "Distance_mi")
          .withColumnRenamed("Wind_Chill(F)", "Wind_Chill")
          .withColumnRenamed("Humidity(%)", "Humidity_Percent")
          .withColumnRenamed("Pressure(in)", "Pressure_in")
          .withColumnRenamed("Visibility(mi)", "Visibility_mi")
          .withColumnRenamed("Wind_Speed(mph)", "Wind_Speed")
          .withColumnRenamed("Precipitation(in)", "Precipitation")
        yearlyDataFrame.cache() // Re-cache after renaming

        yearlyDataFrames(year) = yearlyDataFrame

        println(s"Columns in DataFrame for year $year: ${yearlyDataFrame.columns.mkString(", ")}")

        //yearlyDataFrame.printSchema()
        yearlyDataFrame.createOrReplaceTempView("accidents")

        AggregateQueryRepository.topTemperatureRangesDuringAccidents(spark)
        AggregateQueryRepository.topWeatherConditionsDuringAccidents(spark)
        AggregateQueryRepository.topStatesForAccidents(spark)
        AggregateQueryRepository.topStatesForHeavyRainAccidents(spark)
        AggregateQueryRepository.topStatesForSnowAccidents(spark)
        AggregateQueryRepository.topStatesForMostlyCloudyAccidents(spark)

        AggregateQueryRepository.topFiveStatesPerSeverityLevel(spark)
        AggregateQueryRepository.hourWhenAccidentsHappenQuery(spark)
        AggregateQueryRepository.daysWhenAccidentsHappenQuery(spark)
      }
    }


    // Example usage: show data for a specific year, e.g., 2019
    //yearlyDataFrames.get(2019).foreach(_.show())

    spark.stop()


  }
}
