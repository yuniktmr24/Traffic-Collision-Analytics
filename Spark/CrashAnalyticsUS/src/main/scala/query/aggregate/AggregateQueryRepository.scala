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

    val result = spark.sql(query)

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

    val result = spark.sql(query)

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

  def rankingYearsByAccidentCountQuery(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Ranking by years when most accidents happened (2016 - 2023)")

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
          WHEN Temperature >= -20 AND Temperature < -15 THEN '-20 to -15'
          WHEN Temperature >= -15 AND Temperature < -10 THEN '-15 to -10'
          WHEN Temperature >= -10 AND Temperature < -5 THEN '-10 to -5'
          WHEN Temperature >= -5 AND Temperature < 0 THEN '-5 to 0'
          WHEN Temperature >= 0 AND Temperature < 5 THEN '0 to 5'
          WHEN Temperature >= 5 AND Temperature < 10 THEN '5 to 10'
          WHEN Temperature >= 10 AND Temperature < 15 THEN '10 to 15'
          WHEN Temperature >= 15 AND Temperature < 20 THEN '15 to 20'
          WHEN Temperature >= 20 AND Temperature < 25 THEN '20 to 25'
          WHEN Temperature >= 25 AND Temperature < 30 THEN '25 to 30'
          WHEN Temperature >= 30 AND Temperature < 35 THEN '30 to 35'
          WHEN Temperature >= 35 AND Temperature < 40 THEN '35 to 40'
          WHEN Temperature >= 40 AND Temperature < 45 THEN '40 to 45'
          WHEN Temperature >= 45 AND Temperature < 50 THEN '45 to 50'
          WHEN Temperature >= 50 AND Temperature < 55 THEN '50 to 55'
          WHEN Temperature >= 55 AND Temperature < 60 THEN '55 to 60'
          WHEN Temperature >= 60 AND Temperature < 65 THEN '60 to 65'
          WHEN Temperature >= 65 AND Temperature < 70 THEN '65 to 70'
          WHEN Temperature >= 70 AND Temperature < 75 THEN '70 to 75'
          WHEN Temperature >= 75 AND Temperature < 80 THEN '75 to 80'
          WHEN Temperature >= 80 AND Temperature < 85 THEN '80 to 85'
          WHEN Temperature >= 85 AND Temperature < 90 THEN '85 to 90'
          WHEN Temperature >= 90 AND Temperature < 95 THEN '90 to 95'
          WHEN Temperature >= 95 AND Temperature < 100 THEN '95 to 100'
          WHEN Temperature >= 100 AND Temperature < 105 THEN '100 to 105'
          WHEN Temperature >= 105 AND Temperature < 110 THEN '105 to 110'
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
