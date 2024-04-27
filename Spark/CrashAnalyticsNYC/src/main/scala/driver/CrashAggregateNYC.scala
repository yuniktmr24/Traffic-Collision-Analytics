package driver
import org.apache.log4j.{Level, Logger}
import store.CrashNYCDataFrame
import schema.NYCCollisionSchema
import utils.FunctUtils
import query.AggregateQueryRepository

object CrashAggregateNYC {
  def main(args: Array[String]): Unit = {
  if (args.length != 1) {
      println("Usage: main <csv_file_path>")
      System.exit(1)
    }

    val startTime1 = System.currentTimeMillis()


    val csvFilePath = args(0)

    val spark = CrashNYCDataFrame.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Read data using the defined schema
//    val df = CrashNYCDataFrame.df

    var df = spark.read
      .option("header", "true") // Assumes the first row is a header
      .schema(NYCCollisionSchema.schema)
      .csv(csvFilePath)

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("nyc_accidents")

    // Example action: show the first few rows of the DataFrame
    // df.show()
    println(s"Number of rows in CSV: ${df.count()}")
    println(s"Number of columns in DataFrame: ${df.columns.length}")

    // count accidents by severity
    FunctUtils.printDashes()
//    println("Severity of accidents (2016 - 2023)")
//    val accidentsBySeverity = df.groupBy("Severity").count()
//    accidentsBySeverity.show()


//    Run aggregate queries for NYC

    val startTime2 = System.currentTimeMillis()
    AggregateQueryRepository.collisionsPerYear(spark)
    AggregateQueryRepository.crashesPerDayOfWeek(spark)
    AggregateQueryRepository.crashesPerHourRanked(spark)
    AggregateQueryRepository.collisionsWithPedestriansPerYear(spark)
    AggregateQueryRepository.collisionsByBoroughAndYear(spark)
    AggregateQueryRepository.crashesWithDifferentVehicleCodesPerYear(spark)

    val endTime = System.currentTimeMillis()

    val totalTime1 = endTime - startTime1
    val totalTime2 = endTime - startTime2

    println(s"===================================query execution time: $totalTime2 milliseconds==========================================================")
    println(s"====================================total execution time: $totalTime1 milliseconds===========================================================")


    // Stop the SparkSession
    spark.stop()
  }
}
