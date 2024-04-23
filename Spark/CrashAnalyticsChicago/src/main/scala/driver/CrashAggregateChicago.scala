package driver
import org.apache.log4j.{Level, Logger}
import store.CrashChicagoDataFrame
import utils.FunctUtils
import query.AggregateQueryRepository

object CrashAggregateChicago {
  def main(args: Array[String]): Unit = {
    val spark = CrashChicagoDataFrame.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Read data using the defined schema
    val df = CrashChicagoDataFrame.df
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("chicago_accidents")

    // Example action: show the first few rows of the DataFrame
    // df.show()
    println(s"Number of rows in CSV: ${df.count()}")
    println(s"Number of columns in DataFrame: ${df.columns.length}")

    // count accidents by severity
    FunctUtils.printDashes()
//    println("Severity of accidents (2016 - 2023)")
//    val accidentsBySeverity = df.groupBy("Severity").count()
//    accidentsBySeverity.show()


//    Run aggregate queries for chicago
    AggregateQueryRepository.collisionsPerYear(spark)
    AggregateQueryRepository.crashesPerDayOfWeek(spark)
    AggregateQueryRepository.crashesPerHourRanked(spark)
    AggregateQueryRepository.crashesPerWeatherCondition(spark)
    AggregateQueryRepository.crashesPerCrashType(spark)
    AggregateQueryRepository.crashesPerRoadDefect(spark)
    AggregateQueryRepository.crashesPerInjury(spark)
    AggregateQueryRepository.crashesPerCause(spark)
    

    // Stop the SparkSession
    spark.stop()
  }
}
