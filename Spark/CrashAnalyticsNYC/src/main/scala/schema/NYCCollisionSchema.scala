package schema

import org.apache.spark.sql.types._

object NYCCollisionSchema {
  val schema: StructType = StructType(Array(
    StructField("CRASH_DATE", StringType, nullable = true),
    StructField("CRASH_TIME", StringType, nullable = true),
    StructField("BOROUGH", StringType, nullable = true),
    StructField("ZIP_CODE", StringType, nullable = true),
    StructField("LATITUDE", DoubleType, nullable = true),
    StructField("LONGITUDE", DoubleType, nullable = true),
    StructField("LOCATION", StringType, nullable = true),
    StructField("ON_STREET_NAME", StringType, nullable = true),
    StructField("CROSS_STREET_NAME", StringType, nullable = true),
    StructField("OFF_STREET_NAME", StringType, nullable = true),
    StructField("NUMBER_OF_PERSONS_INJURED", DoubleType, nullable = true),
    StructField("NUMBER_OF_PERSONS_KILLED", DoubleType, nullable = true),
    StructField("NUMBER_OF_PEDESTRIANS_INJURED", IntegerType, nullable = true),
    StructField("NUMBER_OF_PEDESTRIANS_KILLED", IntegerType, nullable = true),
    StructField("NUMBER_OF_CYCLIST_INJURED", IntegerType, nullable = true),
    StructField("NUMBER_OF_CYCLIST_KILLED", IntegerType, nullable = true),
    StructField("NUMBER_OF_MOTORIST_INJURED", IntegerType, nullable = true),
    StructField("NUMBER_OF_MOTORIST_KILLED", IntegerType, nullable = true),
    StructField("CONTRIBUTING_FACTOR_VEHICLE_1", StringType, nullable = true),
    StructField("CONTRIBUTING_FACTOR_VEHICLE_2", StringType, nullable = true),
    StructField("CONTRIBUTING_FACTOR_VEHICLE_3", StringType, nullable = true),
    StructField("CONTRIBUTING_FACTOR_VEHICLE_4", StringType, nullable = true),
    StructField("CONTRIBUTING_FACTOR_VEHICLE_5", StringType, nullable = true),
    StructField("COLLISION_ID", IntegerType, nullable = true),
    StructField("VEHICLE_TYPE_CODE_1", StringType, nullable = true),
    StructField("VEHICLE_TYPE_CODE_2", StringType, nullable = true),
    StructField("VEHICLE_TYPE_CODE_3", StringType, nullable = true),
    StructField("VEHICLE_TYPE_CODE_4", StringType, nullable = true),
    StructField("VEHICLE_TYPE_CODE_5", StringType, nullable = true)
  ))
}
