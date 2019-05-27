package com.civicboost.spark.etl.utilities

import com.civicboost.spark.etl.utilities.GeneralFunctions.{
  cleanString,
  emptyStringToNull,
  generateUUID,
  mapBooleansYNU,
  stringIsNumber,
  stringToDoublePeriodForDecimal,
  stringToDoubleCommaForDecimal
}
import DateTimeFunctions.{
  normalizeDate_dm,
  normalizeDate_md,
  normalizeTimestamp_dm,
  normalizeTimestamp_md
}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


object GeneralUDFs {

  def cleanString_UDF: UserDefinedFunction = udf(cleanString _)

  def emptyStringToNull_UDF: UserDefinedFunction = udf(emptyStringToNull _)

  def generateUUID_UDF: UserDefinedFunction = udf(generateUUID _)

  def mapBooleansYNU_UDF: UserDefinedFunction = udf(mapBooleansYNU _)

  def stringIsNumber_UDF: UserDefinedFunction = udf(stringIsNumber _)

  def stringToDoublePeriodForDecimal_UDF: UserDefinedFunction = udf(stringToDoublePeriodForDecimal _)

  def stringToDoubleCommaForDecimal_UDF: UserDefinedFunction = udf(stringToDoubleCommaForDecimal _)

}


object DateTimeUDFs {

  def normalizeDateMD_UDF: UserDefinedFunction = udf(normalizeDate_md _)

  def normalizeDateDM_UDF: UserDefinedFunction = udf(normalizeDate_dm _)

  def normalizeTimestampMD_UDF: UserDefinedFunction = udf(normalizeTimestamp_md _)

  def normalizeTimestampDM_UDF: UserDefinedFunction = udf(normalizeTimestamp_dm _)

}


object ScalaUDFs {
  /** Normalize string to date.
    *
    * @param dmOrder order of day and month in date string. `DM` = day before month, `MD` = month before day (default)
    * @return
    */
  def normalizeDate_UDF(dmOrder: String): UserDefinedFunction = udf((dateStr: String) =>
    if (dmOrder == "DM") normalizeDate_dm(dateStr)
    else normalizeDate_md(dateStr))

  /** Normalize string to timestamp.
    *
    * @param dmOrder order of day and month in date string. `DM` = day before month, `MD` = month before day (default)
    * @return
    */
  def normalizeTimestamp_UDF(dmOrder: String): UserDefinedFunction = udf((dateTimeStr: String) =>
    if (dmOrder == "DM") normalizeTimestamp_dm(dateTimeStr)
    else normalizeTimestamp_md(dateTimeStr))

}
