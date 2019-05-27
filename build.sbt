name := "spark-etl-utilities"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.9.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.9.0", 
  "org.apache.spark" % "spark-core_2.11" % "2.4.0", 
  "org.apache.spark" % "spark-sql_2.11" % "2.4.0",
  "com.amazonaws" % "aws-java-sdk" % "1.11.469",
  "org.apache.hadoop" % "hadoop-aws" % "2.9.0",
  "com.google.guava" % "guava" % "27.0.1-jre",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

scalacOptions += "-deprecation"
