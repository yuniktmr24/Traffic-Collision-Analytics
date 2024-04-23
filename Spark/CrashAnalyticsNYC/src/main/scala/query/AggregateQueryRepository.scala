package query

import org.apache.spark.sql.SparkSession

object AggregateQueryRepository {
  def collisionsPerYear(spark: SparkSession): Unit = {
//    FunctUtils.printDashes()
    println("Number of Collisions Per Year")

    val collisionsByYear = spark.sql("""
    SELECT SUBSTRING(CRASH_DATE, 7, 4) AS Year, COUNT(*) AS Collision_Count
    FROM nyc_accidents
    WHERE SUBSTRING(CRASH_DATE, 7,4) BETWEEN '2016' AND '2023'
    GROUP BY Year
    ORDER BY Year
    """)

    collisionsByYear.show()
  }

  def crashesPerDayOfWeek(spark: SparkSession): Unit = {
    println("Number of Crashes Per Day of the Week")

    val collisionsByDayOfWeek = spark.sql("""
    SELECT date_format(to_date(CRASH_DATE, 'MM/dd/yyyy'), 'EEEE') AS Day_of_Week,
           COUNT(*) AS Crash_Count
    FROM nyc_accidents
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
      HOUR(CAST(CRASH_TIME AS TIMESTAMP)) AS Hour_of_Day,
      COUNT(*) AS Crash_Count
    FROM 
      nyc_accidents
      WHERE SUBSTRING(CRASH_DATE, 7,4) BETWEEN '2016' AND '2023'
    GROUP BY 
      Hour_of_Day
    ORDER BY 
      Crash_Count DESC
    """)

    crashesPerHour.show()
  }

  def collisionsByBoroughAndYear(spark: SparkSession): Unit = {
  println("Number of Collisions by Borough and Year")

  val collisionsByBoroughAndYear = spark.sql(
    """
    SELECT LOWER(BOROUGH) AS LOW_BOROUGH, COUNT(*) AS Collision_Count
    FROM nyc_accidents
    WHERE BOROUGH IS NOT NULL 
    AND SUBSTRING(CRASH_DATE, 7,4) BETWEEN '2016' AND '2023'

    GROUP BY  BOROUGH
    ORDER BY Collision_Count DESC
    """)

  collisionsByBoroughAndYear.show()
}


  def collisionsWithPedestriansPerYear(spark: SparkSession): Unit = {
  println("Number of Collisions Involving Pedestrians Per Year")

  val collisionsWithPedestrians = spark.sql(
    """
    SELECT SUBSTRING(CRASH_DATE, 7, 4) AS Year, SUM(NUMBER_OF_PEDESTRIANS_INJURED + NUMBER_OF_PEDESTRIANS_KILLED) AS Pedestrian_Collision_Count
    FROM nyc_accidents
    WHERE SUBSTRING(CRASH_DATE, 7,4) BETWEEN '2016' AND '2023'
    GROUP BY Year
    ORDER BY Year
    """)

  collisionsWithPedestrians.show()
}

def crashesWithDifferentVehicleCodesPerYear(spark: SparkSession): Unit = {
  println("Number of Crashes with Different Vehicle Codes")

  val crashesByYearAndVehicleCode = spark.sql(
    """
    SELECT
      LOWER(VEHICLE_TYPE_CODE_1) AS Vehicle_Code,
      COUNT(*) AS Crash_Count
    FROM 
      nyc_accidents
    WHERE 
      VEHICLE_TYPE_CODE_1 IS NOT NULL
      AND SUBSTRING(CRASH_DATE, 7,4) BETWEEN '2016' AND '2023'
    GROUP BY 
      Vehicle_Code
    ORDER BY 
      Crash_Count DESC
    """)

  crashesByYearAndVehicleCode.show()
}

}
