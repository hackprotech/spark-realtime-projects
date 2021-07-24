package com.hackprotech

import org.apache.spark.SparkContext

object HelloWorldSpark extends App {

  val sparkContext = new SparkContext("local", "Hello World")
  val sourceRDD = sparkContext.textFile("D:\\Projects\\BigData\\DataSets\\sample_input.txt")
  sourceRDD.take(1).foreach(println)

}
