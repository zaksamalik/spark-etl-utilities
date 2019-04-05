package com.spark.etl.utilities

import com.spark.etl.utilities.generalFunctions.{cleanString, stringToDouble, mapBooleans}
import com.spark.etl.utilities.dateTimeFunctions.{
  normalizeDate_dm,
  normalizeDate_md,
  normalizeTimestamp_dm,
  normalizeTimestamp_md
}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


object pythonUDFs {

  def cleanStringUDF: UserDefinedFunction = udf(cleanString _)

  def stringToDoubleUDF: UserDefinedFunction = udf(stringToDouble _)

  def mapBooleanValuesUDF: UserDefinedFunction = udf(mapBooleans _)

  def normalizeDateUDF_md: UserDefinedFunction = udf(normalizeDate_md _)

  def normalizeDateUDF_dm: UserDefinedFunction = udf(normalizeDate_dm _)

  def normalizeTimestampUDF_md: UserDefinedFunction = udf(normalizeTimestamp_md _)

  def normalizeTimestampUDF_dm: UserDefinedFunction = udf(normalizeTimestamp_dm _)

}


object scalaUDFs {
  /**
    * normalize string to date.
    *
    * @param dmOrder order of day and month in date string. `DM` = day before month, `MD` = month before day (default)
    * @return
    */
  def normalizeDateUDF(dmOrder: String): UserDefinedFunction = udf((dateStr: String) =>
    if (dmOrder == "DM") normalizeDate_dm(dateStr)
    else normalizeDate_md(dateStr))

  /**
    * normalize string to timestamp.
    *
    * @param dmOrder order of day and month in date string. `DM` = day before month, `MD` = month before day (default)
    * @return
    */
  def normalizeTimestampUDF(dmOrder: String): UserDefinedFunction = udf((dateTimeStr: String) =>
    if (dmOrder == "DM") normalizeTimestamp_dm(dateTimeStr)
    else normalizeTimestamp_md(dateTimeStr))

}
