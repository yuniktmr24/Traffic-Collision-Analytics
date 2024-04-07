package driver

import org.apache.log4j.{Level, Logger}
import store.CrashAggregateDataframe

//aggregate analysis from 2016 - 2023
object CrashAggregate {
  def printDashes(): Unit = {
    println("-----------------------------------------")
  }

  def main(args: Array[String]): Unit = {
    val spark = CrashAggregateDataframe.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Read data using the defined schema
    val df = CrashAggregateDataframe.df

    // Example action: show the first few rows of the DataFrame
    //df.show()
    println(s"Number of rows in CSV: ${df.count()}")
    println(s"Number of columns in DataFrame: ${df.columns.length}")

    //count accidents by severity
    printDashes()
    println("Severity of accidents (2016 - 2023)")
    val accidentsBySeverity = df.groupBy("Severity").count()
    accidentsBySeverity.show()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("accidents")

    // SQL Query to find top 5 states for each severity level using a subquery for window function
    // 2016 - 2023 cumulative
    val query =
      """
         SELECT Severity, State, TotalAccidents
         FROM (
           SELECT Severity, State, count(*) AS TotalAccidents,
             ROW_NUMBER() OVER (PARTITION BY Severity ORDER BY count(*) DESC) AS rank
           FROM accidents
           GROUP BY Severity, State
         ) ranked
         WHERE ranked.rank <= 5
         ORDER BY Severity, TotalAccidents DESC
       """

    // Execute SQL Query
    val result = spark.sql(query)

    printDashes()
    println("States ranked by Accident Severity Ratings (2016 - 2023)")
    result.show(30);
    printDashes()

    // Stop the SparkSession
    spark.stop()
  }
}
