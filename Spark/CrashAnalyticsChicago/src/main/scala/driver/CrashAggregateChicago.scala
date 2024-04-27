package driver
import org.apache.log4j.{Level, Logger}
import store.CrashChicagoDataFrame
import utils.FunctUtils
import query.AggregateQueryRepository
import schema.ChicagoCollisionSchema

object CrashAggregateChicago {
  def main(args: Array[String]): Unit = {
      if (args.length != 1) {
      println("Usage: main <csv_file_path>")
      System.exit(1)
    }

    val startTime1 = System.currentTimeMillis()


    val spark = CrashChicagoDataFrame.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Read data using the defined schema
    // val df = CrashChicagoDataFrame.df
    val csvFilePath = args(0)

        var df = spark.read
      .option("header", "true") // Assumes the first row is a header
      .schema(ChicagoCollisionSchema.schema)
      .csv(csvFilePath)
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

    val startTime2 = System.currentTimeMillis()
//    Run aggregate queries for chicago
    AggregateQueryRepository.collisionsPerYear(spark)
    AggregateQueryRepository.crashesPerDayOfWeek(spark)
    AggregateQueryRepository.crashesPerHourRanked(spark)
    AggregateQueryRepository.crashesPerWeatherCondition(spark)
    AggregateQueryRepository.crashesPerCrashType(spark)
    AggregateQueryRepository.crashesPerRoadDefect(spark)
    AggregateQueryRepository.crashesPerInjury(spark)
    AggregateQueryRepository.crashesPerCause(spark)

    val endTime = System.currentTimeMillis()

    val totalTime1 = endTime - startTime1
    val totalTime2 = endTime - startTime2

    println(s"===================================query execution time: $totalTime2 milliseconds==========================================================")
    println(s"====================================total execution time: $totalTime1 milliseconds===========================================================")

    // Stop the SparkSession
    spark.stop()
  }
}
