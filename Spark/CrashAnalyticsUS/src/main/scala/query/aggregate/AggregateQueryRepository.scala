package query.aggregate

import org.apache.spark.sql.SparkSession
import util.FunctUtils

object AggregateQueryRepository {

  def topFiveStatesPerSeverityLevel(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("States ranked by Accident Severity Ratings (2016 - 2023)")
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
    var result = spark.sql(query)
    result.show(30);

    FunctUtils.printDashes()
  }

  def hourWhenAccidentsHappenQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by hour when most accidents happen (2016 - 2023)")
    // SQL Query to find out during which hour most collisions happen
    val query =
      """
          SELECT HOUR(Start_Time) AS Hour, COUNT(*) AS TotalCollisions
          FROM accidents
          GROUP BY HOUR(Start_Time)
          ORDER BY TotalCollisions DESC
        """

    // Execute SQL Query
    val result = spark.sql(query)

    // Show the results
    result.show(24) // Display all hours
    FunctUtils.printDashes()
  }

  def daysWhenAccidentsHappenQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by day of the week when most accidents happen (2016 - 2023)")

    // SQL Query to find out on which day of the week most collisions happen
    val query =
      """
        SELECT date_format(Start_Time, 'EEEE') AS DayOfWeek, COUNT(*) AS TotalCollisions
        FROM accidents
        GROUP BY DayOfWeek
        ORDER BY TotalCollisions DESC
      """

    // Execute SQL Query
    val result = spark.sql(query)

    // Show the results
    result.show(7) // Display all days of the week
    FunctUtils.printDashes()
  }

  def citiesWhereAccidentsHappenQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by cities where most accidents happen (2016 - 2023)")

    val query =
      """
          SELECT City, COUNT(*) AS TotalAccidents,
                 RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank
          FROM accidents
          WHERE City IS NOT NULL
          GROUP BY City
          ORDER BY TotalAccidents DESC
        """

    // Execute SQL Query
    val result = spark.sql(query)

    // Show the results
    result.show(15) // Display the top 50 cities
    FunctUtils.printDashes()
  }

  def statesWhereAccidentsHappenQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by states where most accidents happen (2016 - 2023)")

    val query =
      """
          SELECT State, COUNT(*) AS TotalAccidents,
                 RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank
          FROM accidents
          GROUP BY State
          ORDER BY TotalAccidents DESC
        """

    // Execute SQL Query
    val result = spark.sql(query)

    // Show the results
    result.show(50)
    FunctUtils.printDashes()
  }
}
