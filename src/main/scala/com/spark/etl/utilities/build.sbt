name := "spark-etl-utilities"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.9.0" % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.9.0" % Provided
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0" % Provided
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0" % Provided
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.469" % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.9.0" % Provided

scalacOptions += "-deprecation"