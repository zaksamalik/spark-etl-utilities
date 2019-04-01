package com.spark.etl.utilities

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import scala.annotation.tailrec
import scala.util.{Failure, Try}

import com.spark.etl.utilities.dateTimeFormats.{
  dateFormats_dm,
  dateFormats_md,
  dateTimeFormats_dm,
  dateTimeFormats_md,
  sparkDateFormatter,
  sparkDateTimeFormatter
}

object baseFunctions {

  /* ~~~~~~~~~~~~~~~~~~~~ Date & Timestamp normalizer functions ~~~~~~~~~~~~~~~~~~~~ */
  /**
    * Clean string for date & datetime parsing
    *
    * @param dtStr date or datetime string
    * @return
    */
  def dtCleaner(dtStr: String): String = dtStr match {
    case null => ""
    case _ => dtStr.trim
  }

  // Modified version of function from Hussachai Puripunpinyo's post: `Normalizing a Date String in the Scala Way.`
  // See: https://medium.com/@hussachai/normalizing-a-date-string-in-the-scala-way-f37a2bdcc4b9
  /**
    * Recursively attempt to normalize string to date or datetime
    *
    * @param dtStr    string to be parsed to date datetime
    * @param patterns list of date datetime patterns and corresponding DateTimeFormatter
    * @return
    */
  @tailrec
  private def normalizeDT(dtStr: String,
                  patterns: List[(String, DateTimeFormatter)]): Try[TemporalAccessor] = patterns match {
    case head :: tail =>
      val resultTry = Try(head._2.parse(dtStr))
      if (resultTry.isSuccess) resultTry else normalizeDT(dtStr, tail)

    case _ => Failure(new RuntimeException("Invalid value passed to function `normalizeDT`: `%s`".format(dtStr)))
  }

  /**
    * Normalize string to date with MONTH before DAY
    *
    * @param dateStr string to be parsed to datetime
    * @return datetime value or None
    */
  def normalizeDate_md(dateStr: String): Option[Date] = {

    val trimmedDate = dtCleaner(dateStr)

    if (trimmedDate.isEmpty) None
    else {
      Some(
        Date.valueOf(normalizeDT(trimmedDate, dateFormats_md).map(sparkDateFormatter.format).get)
      )
    }
  }

  /**
    * Normalize string to date with DAY before MONTH
    *
    * @param dateStr string to be parsed to datetime
    * @return datetime value or None
    */
  def normalizeDate_dm(dateStr: String): Option[Date] = {

    val trimmedDate = dtCleaner(dateStr)

    if (trimmedDate.isEmpty) None
    else {
      Some(
        Date.valueOf(normalizeDT(trimmedDate, dateFormats_dm).map(sparkDateFormatter.format).get)
      )
    }
  }

  /**
    * Normalize string to datetime with MONTH before DAY
    *
    * @param dateTimeStr string to be parsed to datetime
    * @return datetime value or None
    */
  def normalizeTimestamp_md(dateTimeStr: String): Option[Timestamp] = {

    val trimmedDateTime = dtCleaner(dateTimeStr)

    if (trimmedDateTime.isEmpty) None
    else {
      Some(
        Timestamp.valueOf(normalizeDT(trimmedDateTime, dateTimeFormats_md).map(sparkDateTimeFormatter.format).get)
      )
    }
  }

  /**
    * Normalize string to datetime with DAY before MONTH
    *
    * @param dateTimeStr string to be parsed to datetime
    * @return datetime value or None
    */
  def normalizeTimestamp_dm(dateTimeStr: String): Option[Timestamp] = {

    val trimmedDateTime = dtCleaner(dateTimeStr)

    if (trimmedDateTime.isEmpty) None
    else {
      Some(
        Timestamp.valueOf(normalizeDT(trimmedDateTime, dateTimeFormats_dm).map(sparkDateTimeFormatter.format).get)
      )
    }
  }

}
