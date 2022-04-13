package com.hackprotech

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(arr: Array[String]): Unit = {

    // To avoid the multiple info logs in console (only for local testing)
    Logger.getLogger("org").setLevel(Level.ERROR)

    //   1) Create SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("Word_Count")

    val spark = new SparkContext(sparkConf)

    //    2) Read The text file
    val sourceRDD: RDD[String] = spark.textFile("src/main/resources/word_count.txt")

    //    3) Extracting only the words
    //    val extractedRDD: RDD[String] = sourceRDD.flatMap(line => line.split(" "))
    val extractedRDD: RDD[String] = sourceRDD.flatMap(_.split(" "))
    //     hello buddies welcome => [hello, buddies, welcome]
    //     hello
    //    buddies

    //    4) Add count to the Each Word
    //    val mappedCountRDD: RDD[(String, Int)] = extractedRDD.map(word => (word, 1))
    val mappedCountRDD: RDD[(String, Int)] = extractedRDD.map((_, 1))

    //    5) Taking the total Count
    //    val finalCountWordsRDD: RDD[(String, Int)] = mappedCountRDD.reduceByKey((value1, value2) => value1 + value2)
    val finalCountWordsRDD: RDD[(String, Int)] = mappedCountRDD.reduceByKey(_ + _)

    //    6) Printing the Final RDD
    finalCountWordsRDD.foreach(println)


  }

}
