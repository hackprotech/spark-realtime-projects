package com.hackprotech

import org.apache.spark.sql.SparkSession

object SparkSessionWithReadCSV {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local").appName("ReadCSV").getOrCreate()

    val sourceInfo = sparkSession.read.csv("src/main/resources/Used_Bikes.csv")

    sourceInfo.show(false)
  }

}
