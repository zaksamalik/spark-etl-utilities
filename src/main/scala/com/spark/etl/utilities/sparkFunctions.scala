package com.spark.etl.utilities

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparkFunctions {

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

}
