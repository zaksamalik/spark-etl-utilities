package com.spark.etl.utilities

import com.spark.etl.utilities.baseFunctions.{normalizeDate_dm, normalizeDate_md, normalizeTimestamp_dm, normalizeTimestamp_md}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object sparkUDFs {
  /**
    * normalize string to date.
    *
    * @param dmOrder order of day and month in date string. `DM` = day before month, `MD` = month before day (default)
    * @return
    */
  def normalize_date_udf(dmOrder: String): UserDefinedFunction = udf((dateStr: String) =>
    if (dmOrder == "DM") normalizeDate_dm(dateStr)
    else normalizeDate_md(dateStr))

  /**
    * normalize string to timestamp.
    *
    * @param dmOrder order of day and month in date string. `DM` = day before month, `MD` = month before day (default)
    * @return
    */
  def normalize_timestamp_udf(dmOrder: String): UserDefinedFunction = udf((dateTimeStr: String) =>
    if (dmOrder == "DM") normalizeTimestamp_dm(dateTimeStr)
    else normalizeTimestamp_md(dateTimeStr))

}
