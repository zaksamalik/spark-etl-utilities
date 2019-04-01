package com.spark.etl.utilities

import com.spark.etl.utilities.baseFunctions.{
  normalizeDate_md,
  normalizeDate_dm,
  normalizeTimestamp_md,
  normalizeTimestamp_dm
}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object pysparkUDFs {

  def normalizeDateUDF_md: UserDefinedFunction = udf(normalizeDate_md _)

  def normalizeDateUDF_dm: UserDefinedFunction = udf(normalizeDate_dm _)

  def normalizeTimestampUDF_md: UserDefinedFunction = udf(normalizeTimestamp_md _)

  def normalizeTimestampUDF_dm: UserDefinedFunction = udf(normalizeTimestamp_dm _)

}
