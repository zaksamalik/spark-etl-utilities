name := "spark-etl-utilities"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.9.0" % "provided",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.9.0" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "com.amazonaws" % "aws-java-sdk" % "1.11.469" % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "2.9.0" % "provided",
  "com.google.guava" % "guava" % "27.0.1-jre" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

scalacOptions += "-deprecation"
