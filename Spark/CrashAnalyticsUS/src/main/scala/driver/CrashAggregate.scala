package driver

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import store.CrashAggregateDataframe
import query.aggregate.AggregateQueryRepository
import util.FunctUtils

//aggregate analysis from 2016 - 2023
object CrashAggregate {
  def main(args: Array[String]): Unit = {
    val spark = CrashAggregateDataframe.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Read data using the defined schema
    val df = CrashAggregateDataframe.df
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("accidents")

    // Example action: show the first few rows of the DataFrame
    //df.show()
    println(s"Number of rows in CSV: ${df.count()}")
    println(s"Number of columns in DataFrame: ${df.columns.length}")

    //count accidents by severity
    FunctUtils.printDashes()
    println("Severity of accidents (2016 - 2023)")
    val accidentsBySeverity = df.groupBy("Severity").count()
    accidentsBySeverity.show()

    /*** RUN THE OTHER ANALYTICS QUERIES
     *
     */
    AggregateQueryRepository.topFiveStatesPerSeverityLevel(spark)
    AggregateQueryRepository.hourWhenAccidentsHappenQuery(spark)
    AggregateQueryRepository.daysWhenAccidentsHappenQuery(spark)
    AggregateQueryRepository.citiesWhereAccidentsHappenQuery(spark)
    AggregateQueryRepository.statesWhereAccidentsHappenQuery(spark)


    // Stop the SparkSession
    spark.stop()
  }


}
