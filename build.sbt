name := "spark_realtime_projects"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
