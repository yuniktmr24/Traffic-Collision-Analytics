package driver

import org.apache.log4j.{Level, Logger}
import store.CrashYearlyDataframe

object CrashYearly {
  def main(args: Array[String]): Unit = {
    val spark = CrashYearlyDataframe.spark

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val df = CrashYearlyDataframe.df
  }
}
