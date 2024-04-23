package driver
import org.apache.log4j.{Level, Logger}
import store.CrashNYCDataFrame
import utils.FunctUtils
import query.AggregateQueryRepository

object CrashAggregateNYC {
  def main(args: Array[String]): Unit = {
    val spark = CrashNYCDataFrame.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Read data using the defined schema
    val df = CrashNYCDataFrame.df
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
    AggregateQueryRepository.collisionsPerYear(spark)
    AggregateQueryRepository.crashesPerDayOfWeek(spark)
    AggregateQueryRepository.crashesPerHourRanked(spark)
    AggregateQueryRepository.collisionsWithPedestriansPerYear(spark)
    AggregateQueryRepository.collisionsByBoroughAndYear(spark)
    AggregateQueryRepository.crashesWithDifferentVehicleCodesPerYear(spark)
    

    // Stop the SparkSession
    spark.stop()
  }
}
