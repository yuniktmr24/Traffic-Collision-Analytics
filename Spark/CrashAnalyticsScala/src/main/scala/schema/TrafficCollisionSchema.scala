package schema

import org.apache.spark.sql.types._

object TrafficCollisionSchema {
  // Define the schema for our data
 val schema: StructType = StructType(Array(
    StructField("ID", StringType, nullable = true),
    StructField("Source", StringType, nullable = true),
    StructField("Severity", IntegerType, nullable = true),
    StructField("Start_Time", TimestampType, nullable = true),
    StructField("End_Time", StringType, nullable = true), // To be converted to Timestamp if necessary
    StructField("Start_Lat", DoubleType, nullable = true),
    StructField("Start_Lng", DoubleType, nullable = true),
    StructField("End_Lat", DoubleType, nullable = true),
    StructField("End_Lng", DoubleType, nullable = true),
    StructField("Distance(mi)", DoubleType, nullable = true),
    StructField("Description", StringType, nullable = true),
    StructField("Street", StringType, nullable = true),
    StructField("City", StringType, nullable = true),
    StructField("County", StringType, nullable = true),
    StructField("State", StringType, nullable = true),
    StructField("Zipcode", StringType, nullable = true),
    StructField("Country", StringType, nullable = true),
    StructField("Timezone", StringType, nullable = true),
    StructField("Airport_Code", StringType, nullable = true),
    StructField("Weather_Timestamp", StringType, nullable = true),
    StructField("Temperature(F)", DoubleType, nullable = true),
    StructField("Wind_Chill(F)", DoubleType, nullable = true),
    StructField("Humidity(%)", DoubleType, nullable = true),
    StructField("Pressure(in)", DoubleType, nullable = true),
    StructField("Visibility(mi)", DoubleType, nullable = true),
    StructField("Wind_Direction", StringType, nullable = true),
    StructField("Wind_Speed(mph)", DoubleType, nullable = true),
    StructField("Precipitation(in)", DoubleType, nullable = true),
    StructField("Weather_Condition", StringType, nullable = true),
    StructField("Amenity", BooleanType, nullable = true),
    StructField("Bump", BooleanType, nullable = true),
    StructField("Crossing", BooleanType, nullable = true),
    StructField("Give_Way", BooleanType, nullable = true),
    StructField("Junction", BooleanType, nullable = true),
    StructField("No_Exit", BooleanType, nullable = true),
    StructField("Railway", BooleanType, nullable = true),
    StructField("Roundabout", BooleanType, nullable = true),
    StructField("Station", BooleanType, nullable = true),
    StructField("Stop", BooleanType, nullable = true),
    StructField("Traffic_Calming", BooleanType, nullable = true),
    StructField("Traffic_Signal", BooleanType, nullable = true),
    StructField("Turning_Loop", BooleanType, nullable = true),
    StructField("Sunrise_Sunset", StringType, nullable = true),
    StructField("Civil_Twilight", StringType, nullable = true),
    StructField("Nautical_Twilight", StringType, nullable = true),
    StructField("Astronomical_Twilight", StringType, nullable = true),
    StructField("year", DoubleType, nullable = true)
  ))
}
