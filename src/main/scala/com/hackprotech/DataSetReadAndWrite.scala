package com.hackprotech

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object DataSetReadAndWrite {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Bike(bike_name: String, age: Double, price: Double)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("DataSetReadAndWrite")
    sparkConf.setMaster("local")

    //    Spark Session
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //    Read the CSV file
    val sourceDF = sparkSession.read.option("header", true).option("inferSchema", true).csv("src/main/resources/Used_Bikes_With_Header.csv")


    val selectedColumnsDF = sourceDF.select("bike_name", "age", "price")


    import sparkSession.implicits._
    val sourceDS: Dataset[Bike] = selectedColumnsDF.as[Bike]

    sourceDS.printSchema()
    sourceDS.show(false)


//    sourceDS.filter("age_test > 3.0").show(false)    - FAIL AT RUNTIME
//    sourceDS.filter(row => row.age_test > 3.0).show(false)   - FAIL AT COMPILE TIME

    sourceDS.write.mode(SaveMode.Overwrite).csv("target/csv_ds")


  }

}
