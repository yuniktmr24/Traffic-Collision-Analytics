package query.aggregate

import org.apache.spark.sql.SparkSession
import util.FunctUtils

object AggregateQueryRepository {

  def topFiveStatesPerSeverityLevel(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("States ranked by Accident Severity Ratings")
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
    println("Ranking by hour when most accidents happen")
    // SQL Query to find out during which hour most collisions happen
    val query =
      """
          SELECT HOUR(Start_Time) AS Hour, COUNT(*) AS TotalCollisions
          FROM accidents
          GROUP BY HOUR(Start_Time)
          ORDER BY TotalCollisions DESC
        """

    val result = spark.sql(query)

    result.show(24) // Display all hours
    FunctUtils.printDashes()
  }

  def daysWhenAccidentsHappenQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by day of the week when most accidents happen")

    // SQL Query to find out on which day of the week most collisions happen
    val query =
      """
        SELECT date_format(Start_Time, 'EEEE') AS DayOfWeek, COUNT(*) AS TotalCollisions
        FROM accidents
        GROUP BY DayOfWeek
        ORDER BY TotalCollisions DESC
      """

    val result = spark.sql(query)

    // Show the results
    result.show(7) // Display all days of the week
    FunctUtils.printDashes()
  }

  def citiesWhereAccidentsHappenQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by cities where most accidents happen")

    val query =
      """
          SELECT City, COUNT(*) AS TotalAccidents,
                 RANK() OVER (ORDER BY COUNT(*) DESC) AS Rank
          FROM accidents
          WHERE City IS NOT NULL
          GROUP BY City
          ORDER BY TotalAccidents DESC
        """

    val result = spark.sql(query)

    result.show(15) // Display the top 50 cities
    FunctUtils.printDashes()
  }

  def statesWhereAccidentsHappenQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by states where most accidents happen")

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

  def rankingYearsByAccidentCountQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by years when most accidents happened")

    spark.sql(
      """
        SELECT *, year(to_timestamp(Start_Time)) as Accident_Year
        FROM accidents
    """).createOrReplaceTempView("enhanced_traffic_data")

    val yearly_crash_counts = spark.sql(
      """
        SELECT Accident_Year, COUNT(*) as Crash_Count
        FROM enhanced_traffic_data
        GROUP BY Accident_Year
        ORDER BY Crash_Count DESC
    """)

    yearly_crash_counts.show()
    FunctUtils.printDashes()
  }

  def topTemperatureRangesDuringAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 20 Temperature Ranges During Accidents")

    // Categorize temperature into bins and count occurrences
    spark.sql(
      """
      SELECT *,
        CASE
          WHEN Temperature < 0 THEN 'Below Zero F'
          WHEN Temperature >= 0 AND Temperature < 6 THEN '0 to 6'
          WHEN Temperature >= 6 AND Temperature < 11 THEN '6 to 11'
          WHEN Temperature >= 11 AND Temperature < 16 THEN '11 to 16'
          WHEN Temperature >= 16 AND Temperature < 21 THEN '16 to 21'
          WHEN Temperature >= 21 AND Temperature < 26 THEN '21 to 26'
          WHEN Temperature >= 26 AND Temperature < 31 THEN '26 to 31'
          WHEN Temperature >= 31 AND Temperature < 36 THEN '31 to 36'
          WHEN Temperature >= 36 AND Temperature < 41 THEN '36 to 41'
          WHEN Temperature >= 41 AND Temperature < 46 THEN '41 to 46'
          WHEN Temperature >= 46 AND Temperature < 51 THEN '46 to 51'
          WHEN Temperature >= 51 AND Temperature < 56 THEN '51 to 56'
          WHEN Temperature >= 56 AND Temperature < 61 THEN '56 to 61'
          WHEN Temperature >= 61 AND Temperature < 66 THEN '61 to 66'
          WHEN Temperature >= 66 AND Temperature < 71 THEN '66 to 71'
          WHEN Temperature >= 71 AND Temperature < 76 THEN '71 to 76'
          WHEN Temperature >= 76 AND Temperature < 81 THEN '76 to 81'
          WHEN Temperature >= 81 AND Temperature < 86 THEN '81 to 86'
          WHEN Temperature >= 86 AND Temperature < 91 THEN '86 to 91'
          WHEN Temperature >= 91 AND Temperature < 96 THEN '91 to 96'
          WHEN Temperature >= 96 AND Temperature < 101 THEN '96 to 101'
          WHEN Temperature >= 101 AND Temperature < 106 THEN '101 to 106'
          WHEN Temperature >= 106 AND Temperature < 111 THEN '106 to 111'
          ELSE 'Above 110 (Too Hot)'
        END AS Temp_Range
      FROM accidents
    """).createOrReplaceTempView("accidents_with_temp_ranges")

    // Query to count and rank temperature ranges
    val topTempRanges = spark.sql(
      """
      SELECT Temp_Range, COUNT(*) AS Count
      FROM accidents_with_temp_ranges
      GROUP BY Temp_Range
      ORDER BY Count DESC
      LIMIT 20
    """)

    // Show the results
    topTempRanges.show()
    FunctUtils.printDashes()
  }

  def topWeatherConditionsDuringAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 20 Weather Conditions During Accidents")

    val topWeatherConditions = spark.sql(
      """
        SELECT Weather_Condition, COUNT(*) AS Count
        FROM accidents
        WHERE Weather_Condition IS NOT NULL
        GROUP BY Weather_Condition
        ORDER BY Count DESC
        LIMIT 20
      """)

    topWeatherConditions.show()
    FunctUtils.printDashes()
  }

  def topStatesForHeavyRainAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 States for Accidents During Heavy Rain")

    spark.sql(
      """
        SELECT State
        FROM accidents
        WHERE Weather_Condition = 'Heavy Rain'
      """).createOrReplaceTempView("heavy_rain_accidents")

    val topStates = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM heavy_rain_accidents
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 10
      """)

    topStates.show()
    FunctUtils.printDashes()
  }

  def topStatesForSnowAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 States for Accidents During Snow")

    spark.sql(
      """
        SELECT State
        FROM accidents
        WHERE Weather_Condition like '%Snow%'
      """).createOrReplaceTempView("Snow_accidents")

    val topStates = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM Snow_accidents
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 10
      """)

    topStates.show()
    FunctUtils.printDashes()
  }

  def topStatesForMostlyCloudyAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 States for Accidents During Mostly cloudy conditions")

    spark.sql(
      """
        SELECT State
        FROM accidents
        WHERE Weather_Condition = 'Mostly Cloudy'
      """).createOrReplaceTempView("mostly_cloudy_accidents")

    val topStates = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM mostly_cloudy_accidents
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 10
      """)

    topStates.show()
    FunctUtils.printDashes()
  }

  def topStatesForFairWeatherAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 States for Accidents During Fair conditions")

    spark.sql(
      """
        SELECT State
        FROM accidents
        WHERE Weather_Condition = 'Fair'
      """).createOrReplaceTempView("fair_weather_accidents")

    val topStates = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM fair_weather_accidents
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 10
      """)

    topStates.show()
    FunctUtils.printDashes()
  }

  def topWeatherConditionsInColorado(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Weather Conditions for Accidents in Colorado")

    // Filter for accidents in Colorado and create a temporary view
    spark.sql(
      """
        SELECT Weather_Condition
        FROM accidents
        WHERE State = 'CO'
      """).createOrReplaceTempView("colorado_accidents")

    val topWeatherConditionsCO = spark.sql(
      """
        SELECT Weather_Condition, COUNT(*) AS Count
        FROM colorado_accidents
        WHERE Weather_Condition IS NOT NULL
        GROUP BY Weather_Condition
        ORDER BY Count DESC
        LIMIT 10
      """)

    topWeatherConditionsCO.show()
    FunctUtils.printDashes()
  }

  def topWindDirectionInColorado(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Wind Directions for Accidents in Colorado")

    // Filter for accidents in Colorado and create a temporary view
    spark.sql(
      """
        SELECT Wind_Direction
        FROM accidents
        WHERE State = 'CO'
      """).createOrReplaceTempView("colorado_accidents")

    // Count occurrences of each weather condition in Colorado and find the top 10
    val topWeatherConditionsCO = spark.sql(
      """
        SELECT Wind_Direction, COUNT(*) AS Count
        FROM colorado_accidents
        WHERE Wind_Direction IS NOT NULL
        GROUP BY Wind_Direction
        ORDER BY Count DESC
        LIMIT 10
      """)

    topWeatherConditionsCO.show()
    FunctUtils.printDashes()
  }

  def topWeatherConditionsForI70Colorado(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Weather Conditions for Accidents on I-70 in Colorado")

    val query = spark.sql(
      """
        SELECT Weather_Condition, COUNT(*) AS Count
        FROM accidents
        WHERE State = 'CO' AND Description LIKE '%I-70%'
        AND Weather_Condition IS NOT NULL
        GROUP BY Weather_Condition
        ORDER BY Count DESC
        LIMIT 10
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topInterstatesForAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Interstates for Accidents")

    val query = spark.sql(
      """
        SELECT Interstate, COUNT(*) AS Count
        FROM (
          SELECT
            CASE
              WHEN Description LIKE '%I-%' THEN substring(Description, instr(Description, 'I-'), locate(' ', Description, instr(Description, 'I-')) - instr(Description, 'I-'))
              WHEN Description LIKE '%Interstate%' THEN substring(Description, instr(Description, 'Interstate'), locate(' ', Description, instr(Description, 'Interstate')) - instr(Description, 'Interstate'))

            END AS Interstate

          FROM accidents
          WHERE (Description LIKE '%I-%' OR Description LIKE '%Interstate%')
        ) AS Filtered
        WHERE Interstate != 'Other'
        GROUP BY Interstate
        ORDER BY Count DESC
        LIMIT 10
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI95Accidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Accidents on I-95")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-95%'
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)


    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 States for Accidents")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 10
      """)


    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI10Accidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Accidents on I-10")


    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-10%'
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI5Accidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Accidents on I-5")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-5%'
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)


    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI75Accidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Accidents on I-75")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-75%'
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI80Accidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Accidents on I-80")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-80%'
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)

    query.show()
    FunctUtils.printDashes()
  }


  def topInterstatesForSnowAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Interstates for Snow Condition Accidents")

    val query = spark.sql(
      """
        SELECT Interstate, COUNT(*) AS Count
        FROM (
          SELECT
            CASE
              WHEN Description LIKE '%I-%' THEN substring(Description, instr(Description, 'I-'), locate(' ', Description, instr(Description, 'I-')) - instr(Description, 'I-'))
              WHEN Description LIKE '%Interstate%' THEN substring(Description, instr(Description, 'Interstate'), locate(' ', Description, instr(Description, 'Interstate')) - instr(Description, 'Interstate'))

            END AS Interstate,
            Weather_Condition
          FROM accidents
          WHERE (Description LIKE '%I-%' OR Description LIKE '%Interstate%')
            AND (Weather_Condition LIKE '%Snow%' OR Weather_Condition LIKE '%Blizzard%')
        ) AS Filtered
        WHERE Interstate != 'Other'
        GROUP BY Interstate
        ORDER BY Count DESC
        LIMIT 10
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI90SnowAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Snow or Blizzard Conditions Accidents on I-90")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-90%'
          AND (Weather_Condition LIKE '%Snow%' OR Weather_Condition LIKE '%Blizzard%')
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI94SnowAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Snow or Blizzard Conditions Accidents on I-94")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-94%'
          AND (Weather_Condition LIKE '%Snow%' OR Weather_Condition LIKE '%Blizzard%')
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI80SnowAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Snow or Blizzard Conditions Accidents on I-80")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-80%'
          AND (Weather_Condition LIKE '%Snow%' OR Weather_Condition LIKE '%Blizzard%')
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI35SnowAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Snow or Blizzard Conditions Accidents on I-35")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-35%'
          AND (Weather_Condition LIKE '%Snow%' OR Weather_Condition LIKE '%Blizzard%')
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForI70SnowAccidents(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 5 States for Snow or Blizzard Conditions Accidents on I-70")

    val query = spark.sql(
      """
        SELECT State, COUNT(*) AS Count
        FROM accidents
        WHERE Description LIKE '%I-70%'
          AND (Weather_Condition LIKE '%Snow%' OR Weather_Condition LIKE '%Blizzard%')
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 5
      """)

    query.show()
    FunctUtils.printDashes()
  }
}
