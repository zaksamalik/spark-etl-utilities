package com.civicboost.spark.etl.utilities

import java.time.format.DateTimeFormatter

object DateTimeFormats {

  // date formats with month before day
  val dateFormats_md: List[(String, DateTimeFormatter)] = List(
    "uuuu-MM-dd",
    "uuuuMMddZ",
    "uuuuMMdd",
    "yyyy-MM-dd G",
    "uuuu-MM-ddXXX",
    "M/d/uu",
    "MM/dd/uu",
    "MM-dd-uu",
    "M-d-uu",
    "MMM d, uuuu",
    "MMMM d, uuuu",
    "EEEE, MMMM d, uuuu",
    "MMM d uuuu",
    "MMMM d uuuu",
    "MM-dd-uuuu",
    "M-d-uuuu",
    "uuuu-MM-ddXXX",
    "MM/dd/uuuu",
    "M/d/uuuu",
    "uuuu/M/d",
    "MMM.dd.uuuu"
  ).map(p => (p, DateTimeFormatter.ofPattern(p)))

  // datetime formats with month before day
  val dateTimeFormats_md: List[(String, DateTimeFormatter)] = List(
    "uuuu-MM-dd HH:mm:ss",
    "uuuu-MM-dd'T'HH:mm:ss",
    "uuuu-MM-dd HH:mm:ss.SSS",
    "uuuu-MM-dd'T'HH:mm:ss.SSS",
    "uuuu-MM-dd HH:mm:ss.SSS'Z'",
    "uuuu-MM-dd'T'HH:mm:ss.SSS'Z'",
    "uuuu-MM-dd HH:mm:ss.SSS'['VV']'",
    "uuuu-MM-dd'T'HH:mm:ss.SSS'['VV']'",
    "uuuu-MM-dd HH:mm:ss.SSSXXX",
    "uuuu-MM-dd'T'HH:mm:ss.SSSXXX",
    "uuuu-MM-dd HH:mm:ssXXX",
    "uuuu-MM-dd'T'HH:mm:ssXXX",
    "uuuu-MM-dd HH:mm:ss.SSSXXX'['VV']'",
    "uuuu-MM-dd'T'HH:mm:ss.SSSXXX'['VV']'",
    "uuuu-MM-dd HH:mm:ssXXX'['VV']'",
    "uuuu-MM-dd'T'HH:mm:ssXXX'['VV']'",
    "M/d/uu h:mm a",
    "MM/dd/uu h:mm a",
    "MM-dd-uu h:mm a",
    "M-d-uu h:mm a",
    "MMM d, uuuu h:mm:ss a",
    "EEEE, MMMM d, uuuu h:mm:ss a z",
    "EEE MMM dd HH:mm:ss z uuuu",
    "MM-dd-uuuu h:mm:ss a",
    "M-d-uuuu h:mm:ss a",
    "uuuu-MM-dd h:mm:ss a",
    "uuuu-M-d h:mm:ss a",
    "uuuu-MM-dd HH:mm:ss.S",
    "MM/dd/uuuu h:mm:ss a",
    "M/d/uuuu h:mm:ss a",
    "MM/dd/uu h:mm:ss a",
    "MM/dd/uu H:mm:ss",
    "M/d/uu H:mm:ss",
    "MM/dd/uuuu h:mm a",
    "M/d/uuuu h:mm a",
    "MM-dd-uu h:mm:ss a",
    "M-d-uu h:mm:ss a",
    "MM-dd-uuuu h:mm a",
    "M-d-uuuu h:mm a",
    "uuuu-MM-dd h:mm a",
    "uuuu-M-d h:mm a"
  ).map(p => (p, DateTimeFormatter.ofPattern(p)))

  // date formats with month before day
  val dateFormats_dm: List[(String, DateTimeFormatter)] = List(
    "dd/MM/uuuu",
    "d/M/uuuu",
    "uuuu-dd-MM"
  ).map(p => (p, DateTimeFormatter.ofPattern(p)))

  // datetime formats with day before month
  val dateTimeFormats_dm: List[(String, DateTimeFormatter)] = List(
    "EEE, d MMM uuuu HH:mm:ss Z",
    "d MMM uuuu HH:mm:ss Z",
    "dd/MM/uuuu h:mm:ss a",
    "d/M/uuuu h:mm:ss a",
    "dd/MM/uuuu h:mm a",
    "d/M/uuuu h:mm a",
    "d/MMM/uuuu H:mm:ss Z",
    "dd/MMM/uu h:mm a"
  ).map(p => (p, DateTimeFormatter.ofPattern(p)))

  // date and date time formatter for Spark
  val sparkDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd")
  val sparkDateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd[ HH:mm:ss.SSS]")

}
