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

    df.withColumn("test", UDFs.normalizeTimestampUDF_md(col("CREATED_ON"))).show()
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

    val normalizeDateUDF_md: UserDefinedFunction = udf[Option[Date], String](normalizeDate_md)
    val normalizeTimestampUDF_md: UserDefinedFunction = udf[Option[Timestamp], String](normalizeTimestamp_md)
    val normalizeDateUDF_dm: UserDefinedFunction = udf[Option[Date], String](normalizeDate_dm)
    val normalizeTimestampUDF_dm: UserDefinedFunction = udf[Option[Timestamp], String](normalizeTimestamp_dm)

  }

  /* Date & DateTime parser functions */
  // below dateTime functions modified from Hussachai Puripunpinyo's post: `Normalizing a Date String in the Scala Way.`
  // https://medium.com/@hussachai/normalizing-a-date-string-in-the-scala-way-f37a2bdcc4b9
  /**
    *
    * @param dateStr  string to be parsed to date time
    * @param patterns list of date date time patterns and corresponding DateTimeFormatter
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
    * @param dateStr string to be parsed to date time
    * @return date time value or None
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
    * Normalize string to date time with MONTH before DAY
    *
    * @param dateStr string to be parsed to date time
    * @return date time value or None
    */
  def normalizeTimestamp_md(dateStr: String): Option[Timestamp] = {

    val trimmedDate = dateStr.trim

    if (trimmedDate.isEmpty) None
    else {
      Some(
        Timestamp.valueOf(normalizeDT(trimmedDate, dateTimeFormats_md).map(sparkDateTimeFormatter.format).get)
      )
    }
  }

  /**
    * Normalize string to date with DAY before MONTH
    *
    * @param dateStr string to be parsed to date time
    * @return date time value or None
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
    * Normalize string to date time with DAY before MONTH
    *
    * @param dateStr string to be parsed to date time
    * @return date time value or None
    */
  def normalizeTimestamp_dm(dateStr: String): Option[Timestamp] = {

    val trimmedDate = dateStr.trim

    if (trimmedDate.isEmpty) None
    else {
      Some(
        Timestamp.valueOf(normalizeDT(trimmedDate, dateTimeFormats_dm).map(sparkDateTimeFormatter.format).get)
      )
    }
  }


}
