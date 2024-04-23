package query

import org.apache.spark.sql.SparkSession

object AggregateQueryRepository {
  def collisionsPerYear(spark: SparkSession): Unit = {
//    FunctUtils.printDashes()
    println("Number of Collisions Per Year")

  val crashesByYear = spark.sql(
    """
    SELECT 
      SUBSTRING(CRASH_DATE, 7, 4) AS Year,
      COUNT(*) AS Crash_Count
    FROM 
      chicago_accidents
    WHERE 
      CRASH_DATE IS NOT NULL AND SUBSTRING(CRASH_DATE, 7,4) BETWEEN '2016' AND '2023'
    GROUP BY 
      Year
    ORDER BY 
      Year
    """)

    crashesByYear.show()
  }

  def crashesPerDayOfWeek(spark: SparkSession): Unit = {
    println("Number of Crashes Per Day of the Week")

    val collisionsByDayOfWeek = spark.sql("""
    SELECT CRASH_DAY_OF_WEEK AS Day_of_Week,
           COUNT(*) AS Crash_Count
    FROM chicago_accidents
    WHERE SUBSTRING(CRASH_DATE, 7,4) BETWEEN '2016' AND '2023'
    GROUP BY Day_of_Week
    ORDER BY Crash_Count DESC
    """)

    collisionsByDayOfWeek.show()
  }

  def crashesPerHourRanked(spark: SparkSession): Unit = {
    println("Number of Crashes Per Hour of the Day (Ranked)")

    val crashesPerHour = spark.sql("""
    SELECT 
      CRASH_HOUR AS Hour_of_Day,
      COUNT(*) AS Crash_Count
    FROM 
      chicago_accidents
    GROUP BY 
      Hour_of_Day
    ORDER BY 
      Crash_Count DESC
    """)

    crashesPerHour.show()
  }




def crashesPerWeatherCondition(spark: SparkSession): Unit = {
  println("Number of Crashes Per Weather Condition")

  val crashesByWeatherCondition = spark.sql(
    """
    SELECT 
      WEATHER_CONDITION,
      COUNT(*) AS Crash_Count
    FROM 
      chicago_accidents
    WHERE 
      WEATHER_CONDITION IS NOT NULL
    GROUP BY 
      WEATHER_CONDITION
    ORDER BY 
      Crash_Count DESC
    """)

  crashesByWeatherCondition.show()
}

def crashesPerCrashType(spark: SparkSession): Unit = {
  println("Number of Crashes Per First Crash Type")

  val crashType = spark.sql(
    """
    SELECT 
      CRASH_TYPE,
      COUNT(*) AS Crash_Count
    FROM 
      chicago_accidents
    WHERE 
      CRASH_TYPE IS NOT NULL
    GROUP BY 
      CRASH_TYPE
    ORDER BY 
      Crash_Count DESC
    """)

  crashType.show()
}

def crashesPerRoadDefect(spark: SparkSession): Unit = {
  println("Number of Crashes Per First Crash Type")

  val roadDefect = spark.sql(
    """
    SELECT 
      ROAD_DEFECT,
      COUNT(*) AS Crash_Count
    FROM 
      chicago_accidents
    WHERE 
      ROAD_DEFECT IS NOT NULL
    GROUP BY 
      1
    ORDER BY 
      Crash_Count DESC
    """)

  roadDefect.show()
}



def crashesPerInjury(spark: SparkSession): Unit = {
  println("Number of Crashes per injury type")

  val crashType = spark.sql(
    """
    SELECT 
      MOST_SEVERE_INJURY,
      COUNT(*) AS Crash_Count
    FROM 
      chicago_accidents
    WHERE 
      MOST_SEVERE_INJURY IS NOT NULL
    GROUP BY 
      1
    ORDER BY 
      Crash_Count DESC
    """)

  crashType.show()
}

def crashesPerCause(spark: SparkSession): Unit = {
  println("Number of Crashes Per contributory cause")

  val crashType = spark.sql(
    """
    SELECT 
      PRIM_CONTRIBUTORY_CAUSE,
      COUNT(*) AS Crash_Count
    FROM 
      chicago_accidents
    WHERE 
      PRIM_CONTRIBUTORY_CAUSE IS NOT NULL
    GROUP BY 
      1
    ORDER BY 
      Crash_Count DESC
    """)

  crashType.show()
}


}
