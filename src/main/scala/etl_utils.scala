import scala.annotation.tailrec
import scala.util.{Failure, Try}
import dateTimeFormats._

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object etl_utils {

  def main(args: Array[String]): Unit = {

    val UDFs = new UDFs

    val conf: SparkConf = new SparkConf().setMaster("local[2]")

    val spark: SparkSession = startSpark(conf = conf, awsProfile = Some("abc"))

    /*
    val df =
      spark
        .read
        .json("s3a://data.open.data/raw/govt/usa/city/pittsburgh/wprdc/_311_service_requests/json")
    df.show()
    println(df.schema)
    */

    val someData = Seq(
      Row("2019-03-25T08:20:00")
    )

    val someSchema = List(
      StructField("CREATED_ON", StringType, nullable = true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(someSchema)
    )

    df.withColumn("test", UDFs.normalizeTimestampUDF("MD")(col("CREATED_ON"))).show()
  }

  /**
    *
    * @param conf       SparkConf()
    * @param enableHive whether to enableHiveSupport
    * @param awsProfile option name of AWS profile to get credentials
    * @return `spark`: instantiated SparkSession
    */
  def startSpark(conf: SparkConf, enableHive: Boolean = false, awsProfile: Option[String] = null): SparkSession = {

    val spark =
      if (enableHive) {
        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      } else {
        SparkSession
          .builder()
          .config(conf)
          .getOrCreate()
      }

    if (awsProfile.isDefined) {
      val awsCredentials = new DefaultAWSCredentialsProviderChain().getCredentials
      val hadoopConfig = spark.sparkContext.hadoopConfiguration
      hadoopConfig.set("fs.s3a.multiobjectdelete.enable", "false")
      hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      hadoopConfig.set("fs.s3a.access.key", awsCredentials.getAWSAccessKeyId)
      hadoopConfig.set("fs.s3a.secret.key", awsCredentials.getAWSSecretKey)
      spark
    }
    else {
      spark
    }
  }

  /* Date & DateTime parser functions */
  class UDFs {

    def normalizeDateUDF(dmOrder: String): UserDefinedFunction = udf((dateStr: String) =>
      if (dmOrder == "DM") normalizeDate_dm(dateStr)
      else normalizeDate_md(dateStr))

    def normalizeTimestampUDF(dmOrder: String): UserDefinedFunction = udf((dateTimeStr: String) =>
      if (dmOrder == "DM") normalizeTimestamp_dm(dateTimeStr)
      else normalizeTimestamp_md( dateTimeStr))

  }

  /* Date & DateTime parser functions */
  // below dateTime functions modified from Hussachai Puripunpinyo's post: `Normalizing a Date String in the Scala Way.`
  // https://medium.com/@hussachai/normalizing-a-date-string-in-the-scala-way-f37a2bdcc4b9
  /**
    *
    * @param dateStr  string to be parsed to datetime
    * @param patterns list of date datetime patterns and corresponding DateTimeFormatter
    * @return
    */
  @tailrec
  def normalizeDT(dateStr: String,
                  patterns: List[(String, DateTimeFormatter)]): Try[TemporalAccessor] = patterns match {
    case head :: tail =>
      val resultTry = Try(head._2.parse(dateStr))
      if (resultTry.isSuccess) resultTry else normalizeDT(dateStr, tail)

    case _ => Failure(new RuntimeException("no match found"))
  }

  /**
    * Normalize string to date with MONTH before DAY
    *
    * @param dateStr string to be parsed to datetime
    * @return datetime value or None
    */
  def normalizeDate_md(dateStr: String): Option[Date] = {

    val trimmedDate = dateStr.trim

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

    val trimmedDate = dateStr.trim

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

    val trimmedDateTime = dateTimeStr.trim

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

    val trimmedDateTime = dateTimeStr.trim

    if (trimmedDateTime.isEmpty) None
    else {
      Some(
        Timestamp.valueOf(normalizeDT(trimmedDateTime, dateTimeFormats_dm).map(sparkDateTimeFormatter.format).get)
      )
    }
  }
}
