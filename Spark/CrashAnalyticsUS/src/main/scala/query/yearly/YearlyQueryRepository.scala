package query.yearly

import org.apache.spark.sql.SparkSession
import util.FunctUtils

object YearlyQueryRepository {
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

  def getTempRangesPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Temperature Ranges Each Year")

    val crashesCountByYearAndWeather = spark.sql("""
    SELECT year,
           Weather_Condition,
           COUNT(*) AS Crashes_Count
    FROM accidents
    GROUP BY year, Weather_Condition
    ORDER BY year, Crashes_Count DESC
""")

// Displaying the result
    crashesCountByYearAndWeather.show()

    FunctUtils.printDashes()

  }

  def getCrashesPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("No. of crashes each year")

    var crashesPerYear = spark.sql(
      "SELECT year, COUNT(*) AS NumCrashes FROM accidents GROUP BY year ORDER BY year"
    )

    // Displaying the result
    crashesPerYear.show()

    FunctUtils.printDashes()
  }

  def rankStatesPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Rank states each year")

    val rankedStatesByYear = spark.sql("""
    SELECT year,
           State,
           ROW_NUMBER() OVER (PARTITION BY year ORDER BY Crashes_Count DESC) AS Rank,
           Crashes_Count
    FROM (
        SELECT year,
               State,
               COUNT(*) AS Crashes_Count
        FROM accidents
        GROUP BY year, State
    ) temp
""")

// Displaying the result
    rankedStatesByYear.show()

    FunctUtils.printDashes()
  }

  def topTempRangesPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 15 Temperature Ranges During Accidents per Year")

    // Categorize temperature into bins and count occurrences
    spark
      .sql("""
    SELECT year,
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

    // Query to count and rank temperature ranges per year
    val topTempRangesPerYear = spark.sql("""
    SELECT year, Temp_Range, COUNT(*) AS Count
    FROM accidents_with_temp_ranges
    GROUP BY year, Temp_Range
    ORDER BY year, Count DESC
  """)

    // Show the results
    topTempRangesPerYear.show()
    FunctUtils.printDashes()
  }

  def topWeatherConditionsPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 20 Weather Conditions During Accidents Per Year")

    val topWeatherConditions = spark.sql("""
    SELECT year, Weather_Condition, COUNT(*) AS Count
    FROM accidents
    WHERE Weather_Condition IS NOT NULL
    GROUP BY year, Weather_Condition
    ORDER BY year, Count DESC
  """)

    topWeatherConditions.show()
    FunctUtils.printDashes()
  }

  def topStatesHeavyRainPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 States for Accidents During Heavy Rain Per Year")

    spark
      .sql("""
    SELECT year, State
    FROM accidents
    WHERE Weather_Condition = 'Heavy Rain'
  """).createOrReplaceTempView("heavy_rain_accidents")

    val topStates = spark.sql("""
    SELECT year, State, COUNT(*) AS Count
    FROM heavy_rain_accidents
    GROUP BY year, State
    ORDER BY year, Count DESC
  """)

    topStates.show()
    FunctUtils.printDashes()
  }
  def topStatesSnowPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 States for Accidents During Snow Per Year")

    spark
      .sql("""
    SELECT year, State
    FROM accidents
    WHERE Weather_Condition LIKE '%Snow%'
  """).createOrReplaceTempView("Snow_accidents")

    val topStates = spark.sql("""
    SELECT year, State, COUNT(*) AS Count
    FROM Snow_accidents
    GROUP BY year, State
    ORDER BY year, Count DESC
  """)

    topStates.show()
    FunctUtils.printDashes()
  }

  def topStatesMostlyCloudyPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println(
      "Top 10 States for Accidents During Mostly Cloudy Conditions Per Year"
    )

    spark
      .sql("""
    SELECT year, State
    FROM accidents
    WHERE Weather_Condition = 'Mostly Cloudy'
  """).createOrReplaceTempView("mostly_cloudy_accidents")

    val topStates = spark.sql("""
    SELECT year, State, COUNT(*) AS Count
    FROM mostly_cloudy_accidents
    GROUP BY year, State
    ORDER BY year, Count DESC
  """)

    topStates.show()
    FunctUtils.printDashes()
  }

  def topStatesFairWeatherPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 States for Accidents During Fair conditions Per Year")

    spark
      .sql("""
        SELECT State
        FROM accidents
        WHERE Weather_Condition = 'Fair'
      """)
      .createOrReplaceTempView("fair_weather_accidents")

    val topStates = spark.sql("""
        SELECT State, COUNT(*) AS Count
        FROM fair_weather_accidents
        GROUP BY State
        ORDER BY Count DESC
        LIMIT 10
      """)

    topStates.show()
    FunctUtils.printDashes()
  }

  def topWeatherCOPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Weather Conditions for Accidents in Colorado Per Year")

    // Filter for accidents in Colorado and create a temporary view
    spark
      .sql("""
    SELECT year, Weather_Condition
    FROM accidents
    WHERE State = 'CO'
  """).createOrReplaceTempView("colorado_accidents")

    val topWeatherConditionsCO = spark.sql("""
    SELECT year, Weather_Condition, COUNT(*) AS Count
    FROM colorado_accidents
    WHERE Weather_Condition IS NOT NULL
    GROUP BY year, Weather_Condition
    ORDER BY year, Count DESC
  """)

    topWeatherConditionsCO.show()
    FunctUtils.printDashes()
  }

  def topWindCOPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Wind Directions for Accidents in Colorado Per Year")

    // Filter for accidents in Colorado and create a temporary view
    spark
      .sql("""
        SELECT Wind_Direction
        FROM accidents
        WHERE State = 'CO'
      """)
      .createOrReplaceTempView("colorado_accidents")

    // Count occurrences of each weather condition in Colorado and find the top 10
    val topWeatherConditionsCO = spark.sql("""
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

  def topWeatherConditionsI70COPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println(
      "Top 10 Weather Conditions for Accidents on I-70 in Colorado Per Year"
    )

    val query = spark.sql("""
    SELECT year, Weather_Condition, COUNT(*) AS Count
    FROM accidents
    WHERE State = 'CO' AND Description LIKE '%I-70%'
    AND Weather_Condition IS NOT NULL
    GROUP BY year, Weather_Condition
    ORDER BY year, Count DESC
  """)

    query.show()
    FunctUtils.printDashes()
  }

  def topInterstatesForAccidentsPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Interstates for Accidents per year")

    val query = spark.sql("""
    SELECT year, Interstate, COUNT(*) AS Count
    FROM (
      SELECT
        year,
        CASE
          WHEN Description LIKE '%I-%' THEN substring(Description, instr(Description, 'I-'), locate(' ', Description, instr(Description, 'I-')) - instr(Description, 'I-'))
          WHEN Description LIKE '%Interstate%' THEN substring(Description, instr(Description, 'Interstate'), locate(' ', Description, instr(Description, 'Interstate')) - instr(Description, 'Interstate'))
        END AS Interstate
      FROM accidents
      WHERE Description LIKE '%I-%' OR Description LIKE '%Interstate%'
    ) AS Filtered
    WHERE Interstate != 'Other'
    GROUP BY year, Interstate
    ORDER BY year, Count DESC
  """)

    query.show()
    FunctUtils.printDashes()
  }

  def topStatesForInterstateAccidentsPerYear(
      spark: SparkSession,
      interstate: String
  ): Unit = {
    FunctUtils.printDashes()
    println(s"Top 5 States for Accidents on $interstate per year")

    val query = spark.sql(s"""
    SELECT year, State, COUNT(*) AS Count
    FROM accidents
    WHERE Description LIKE '%$interstate%'
    GROUP BY year, State
    ORDER BY year, Count DESC
    LIMIT 5
  """)

    query.show()
    FunctUtils.printDashes()
  }

  def topInterstatesForSnowAccidentsPerYear(spark: SparkSession): Unit = {
    FunctUtils.printDashes()
    println("Top 10 Interstates for Snow Condition Accidents per year")

    val query = spark.sql("""
    SELECT year, Interstate, COUNT(*) AS Count
    FROM (
      SELECT
        year,
        CASE
          WHEN Description LIKE '%I-%' THEN substring(Description, instr(Description, 'I-'), locate(' ', Description, instr(Description, 'I-')) - instr(Description, 'I-'))
          WHEN Description LIKE '%Interstate%' THEN substring(Description, instr(Description, 'Interstate'), locate(' ', Description, instr(Description, 'Interstate')) - instr(Description, 'Interstate'))
        END AS Interstate
      FROM accidents
      WHERE (Description LIKE '%I-%' OR Description LIKE '%Interstate%')
        AND (Weather_Condition LIKE '%Snow%' OR Weather_Condition LIKE '%Blizzard%')
    ) AS Filtered
    WHERE Interstate != 'Other'
    GROUP BY year, Interstate
    ORDER BY year, Count DESC
  """)

    query.show()
    FunctUtils.printDashes()
  }

}
