package com.hackprotech

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrameReadAndWrite {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("DataFrameReadAndWrite")
    sparkConf.setMaster("local")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //    Read CSV
    val sourceBikeDF = sparkSession.read.option("header", true).option("inferSchema", true).csv("src/main/resources/Used_Bikes_With_Header.csv")
    //    sourceBikeDF.show()

    //    Manipulate DF
    val extractedDF = sourceBikeDF.select("bike_name", "brand", "owner").where(col("owner") === "First Owner")

    //    FOR REFERENCE
    //    val extractedDF = sourceBikeDF.select("bike_name", "brand", "owner")
    //          .where(col("owner") === "First Owner")
    //          .filter("brand == 'TVS'")       ----- Runtime Evaluation

    //    extractedDF.show(extractedDF.count().toInt, false)
    println(sourceBikeDF.count())
    println(extractedDF.count())

    sourceBikeDF.printSchema()



    //    Write DF to local Path
    extractedDF.write.mode(SaveMode.Overwrite).csv("target/csv_op")

    sparkSession.stop()

  }

}
