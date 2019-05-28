package com.civicboost.spark.etl.utilities

import com.google.common.base.CharMatcher
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor
import java.util.UUID.randomUUID
import scala.annotation.tailrec
import scala.util.{Failure, Try}

import DateTimeFormats.{
  dateFormats_dm,
  dateFormats_md,
  dateTimeFormats_dm,
  dateTimeFormats_md,
  sparkDateFormatter,
  sparkDateTimeFormatter
}


object GeneralFunctions {

  /** Remove Java ISO control characters from, and trim, string.
    *
    * @param str string to clean
    * @return
    */
  def cleanString(str: String): String = str match {
    case null => null
    case s => CharMatcher.javaIsoControl.removeFrom(s).trim
  }

  /** Convert empty strings to null values.
    *
    * @param str string to check if empty
    * @return
    */
  def emptyStringToNull(str: String): String = str match {
    case null => null
    case s => s.trim match {
      case "" => null
      case _ => s
    }
  }

  /** Generate V4 UUID
    *
    * @return
    */
  def generateUUID(): String = {
    randomUUID().toString
  }

  /** Map boolean values to `Y`, `N`, `Unknown`
    *
    * @param boolVal boolean-indicator value to map to indicator values. Can be Integer, string or boolean
    * @return
    */
  def mapBooleansYNU(boolVal: Any): String = boolVal match {
    case false | 0 | "0" | "f" | "F" | "false" | "False" | "FALSE" | "n" | "N" | "no" | "No" | "NO" => "N"
    case true | 1 | "1" | "t" | "T" | "true" | "True" | "TRUE" | "y" | "Y" | "yes" | "Yes" | "YES" => "Y"
    case null => "Unknown"
    case _ => "Unknown"
  }

  /** Convert string to double where period represents decimal place.
    *
    * @param str double value coded as string
    * @return
    */
  def stringToDoublePeriodForDecimal(str: String): Option[Double] = {
    stringToDoubleBase(str)
  }

  /** Convert string to decimal where commas represents decimal place.
    *
    * @param str double value coded as string
    * @return
    */
  def stringToDoubleCommaForDecimal(str: String): Option[Double] = {
    stringToDoubleBase(str, comma_for_decimal = true)
  }

  /** Convert string representing a number to Double.
    *
    * @param str double value coded as string
    * @return
    */
  def stringToDoubleBase(str: String, comma_for_decimal: Boolean = false): Option[Double] = str match {
    case null => null
    case s: String =>
      if (!stringIsNumber(s)) null
      else {
        // remove all chars except for numbers, commas, decimals, and negative signs
        val string_clean =
          if (comma_for_decimal) {
            s.trim.replaceAll("[^0-9,-]", "").replaceAll(",", ".")
          } else {
            s.trim.replaceAll("[^0-9.-]", "")
          }
        // get double value
        val number_match = extractNumberString(string_clean).toDouble
        if (s.matches("\\(.*\\)")) {
          Some(number_match * -1.0)
        } else {
          Some(number_match)
        }
      }
  }

  /** Extract number string (if any) from string.
    *
    * @param str string containing number
    * @return
    */
  def extractNumberString(str: String): String = {
    val number_pattern = "(\\-?[0-9]+(\\.[0-9]+)?)".r
    number_pattern.findFirstMatchIn(str).getOrElse("ERROR: BAD NUMBER PARSING").toString
  }

  /** Validate whether passed string could be converted to a number.
    *
    * @param str string to check for number format
    * @return
    */
  def stringIsNumber(str: String): Boolean = str match {
    case null => false
    case _ => str.trim.replaceAll("[^0-9]", "").matches("^\\d+$")
  }
}


object DateTimeFunctions {

  /* ~~~~~~~~~~~~~~~~~~~~ Date & Timestamp normalizer functions ~~~~~~~~~~~~~~~~~~~~ */
  /** Normalize string to date with MONTH before DAY
    *
    * @param dateStr string to be parsed to datetime
    * @return datetime value or null
    */
  def normalizeDate_md(dateStr: String): Date = {
    Date.valueOf(getDateOrTimestamp(dateStr, dateFormats_md ::: dateTimeFormats_md, sparkDateFormatter))
  }

  /** Normalize string to date with DAY before MONTH
    *
    * @param dateStr string to be parsed to datetime
    * @return datetime value or null
    */
  def normalizeDate_dm(dateStr: String): Date = {
    Date.valueOf(getDateOrTimestamp(dateStr, dateFormats_dm ::: dateTimeFormats_dm, sparkDateFormatter))
  }

  /** Normalize string to datetime with MONTH before DAY
    *
    * @param dateTimeStr string to be parsed to datetime
    * @return datetime value or None
    */
  def normalizeTimestamp_md(dateTimeStr: String): Timestamp = {

    val timestampString = getDateOrTimestamp(dateTimeStr, dateTimeFormats_md ::: dateFormats_md, sparkDateTimeFormatter)

    getTimestamp(timestampString)
  }

  /** Normalize string to datetime with DAY before MONTH
    *
    * @param dateTimeStr string to be parsed to datetime
    * @return datetime value or None
    */
  def normalizeTimestamp_dm(dateTimeStr: String): Timestamp = {

    val timestampString = getDateOrTimestamp(dateTimeStr, dateTimeFormats_dm ::: dateFormats_dm, sparkDateTimeFormatter)

    getTimestamp(timestampString)
  }

  /** Get timestamp value of string.
    *
    * @param tsString timestamp in string format.
    * @return
    */
  def getTimestamp(tsString: String): Timestamp = tsString match {
    case null => null
    case _ =>
      if (tsString.length == 10) {
        Timestamp.valueOf(tsString + " 00:00:00.000")
      }
      else {
        Timestamp.valueOf(tsString)
      }
  }

  /** Get date or timestamp from string
    *
    * @param dateTimeString string containing date or timestamp
    * @param dateTimeFormats datetime formats to match against
    * @param dateTimeFormatter datetime formatter
    * @return
    */
  def getDateOrTimestamp(dateTimeString: String,
                         dateTimeFormats: List[(String, DateTimeFormatter)],
                         dateTimeFormatter: DateTimeFormatter): String = {

    val trimmedDateTime = dtCleaner(dateTimeString)

    if (trimmedDateTime.isEmpty) null
    else {
      normalizeDT(trimmedDateTime, dateTimeFormats).map(dateTimeFormatter.format).get
    }
  }

  // Modified version of function from Hussachai Puripunpinyo's post: `Normalizing a Date String in the Scala Way.`
  // See: https://medium.com/@hussachai/normalizing-a-date-string-in-the-scala-way-f37a2bdcc4b9
  /** Recursively attempt to normalize string to date or datetime
    *
    * @param dtStr    string to be parsed to date datetime
    * @param patterns list of date datetime patterns and corresponding DateTimeFormatter
    * @return
    */
  @tailrec
  def normalizeDT(dtStr: String, patterns: List[(String, DateTimeFormatter)]): Try[TemporalAccessor] = patterns match {
    case head :: tail =>
      val resultTry = Try(head._2.parse(dtStr))
      if (resultTry.isSuccess) resultTry else normalizeDT(dtStr, tail)
    case _ => Failure(new RuntimeException("Invalid value passed to function `normalizeDT`: `%s`".format(dtStr)))
  }

  /** Clean string for date & datetime parsing
    *
    * @param dtStr date or datetime string
    * @return
    */
  def dtCleaner(dtStr: String): String = dtStr match {
    case null => ""
    case s => s.trim
  }

}
