package com.hackprotech

import org.apache.spark.{SparkConf, SparkContext}

object SparkBasics extends App {
  // Create a configuration
  val sparkConf = new SparkConf()
  sparkConf.setAppName("SparkBasics")
  sparkConf.setMaster("local")

  //  SparkSession or SparkContext
  val sparkContext = new SparkContext(sparkConf)

  //  Create Distributed Datasets
  val sourceRDD = sparkContext.parallelize(Array(1,2,3,4,5))

  //  Print it in the console
  sourceRDD.foreach(println)

  sparkContext.stop()
}
