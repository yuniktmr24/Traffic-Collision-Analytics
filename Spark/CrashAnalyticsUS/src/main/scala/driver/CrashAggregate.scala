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

    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)

    var csvFilePath = ""
    if (args.length != 0) {
      csvFilePath = args(0)
    }
    val startTime1 = System.currentTimeMillis()

    // Read data using the defined schema
    val df = CrashAggregateDataframe.loadDataFrame(csvFilePath)
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

    val startTime2 = System.currentTimeMillis()


    AggregateQueryRepository.topFiveStatesPerSeverityLevel(spark)
    AggregateQueryRepository.hourWhenAccidentsHappenQuery(spark)
    AggregateQueryRepository.daysWhenAccidentsHappenQuery(spark)
    AggregateQueryRepository.citiesWhereAccidentsHappenQuery(spark)
    AggregateQueryRepository.statesWhereAccidentsHappenQuery(spark)
    AggregateQueryRepository.rankingYearsByAccidentCountQuery(spark)
    AggregateQueryRepository.topTemperatureRangesDuringAccidents(spark)


    /**Weather queries*/
//    AggregateQueryRepository.topWeatherConditionsDuringAccidents(spark)
//    AggregateQueryRepository.topStatesForHeavyRainAccidents(spark)
//    AggregateQueryRepository.topStatesForSnowAccidents(spark)
//    AggregateQueryRepository.topStatesForMostlyCloudyAccidents(spark)
//    AggregateQueryRepository.topStatesForFairWeatherAccidents(spark)
//
//
//    /**CO Queries*/
//    AggregateQueryRepository.topWeatherConditionsInColorado(spark)
//    AggregateQueryRepository.topWindDirectionInColorado(spark)
//    AggregateQueryRepository.topWeatherConditionsForI70Colorado(spark)
//
//
//    /**Interstates in General*/
//    AggregateQueryRepository.topInterstatesForAccidents(spark)
//    AggregateQueryRepository.topStatesForI95Accidents(spark)
//    AggregateQueryRepository.topStatesForI10Accidents(spark)
//    AggregateQueryRepository.topStatesForI5Accidents(spark)
//    AggregateQueryRepository.topStatesForI75Accidents(spark)
//    AggregateQueryRepository.topStatesForI80Accidents(spark)
//
//    /**Interstates in snow*/
//    AggregateQueryRepository.topInterstatesForSnowAccidents(spark)
//    AggregateQueryRepository.topStatesForI90SnowAccidents(spark)
//    AggregateQueryRepository.topStatesForI94SnowAccidents(spark)
//    AggregateQueryRepository.topStatesForI80SnowAccidents(spark)
//    AggregateQueryRepository.topStatesForI35SnowAccidents(spark)
//    AggregateQueryRepository.topStatesForI70SnowAccidents(spark)

    val endTime = System.currentTimeMillis()

    val totalTime1 = endTime - startTime1
    val totalTime2 = endTime - startTime2

    println(s"===================================query execution time: $totalTime2 milliseconds==========================================================")
    println(s"====================================total execution time: $totalTime1 milliseconds===========================================================")

    // Stop the SparkSession
    spark.stop()
  }


}
