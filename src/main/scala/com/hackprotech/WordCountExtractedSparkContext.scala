package com.hackprotech

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountExtractedSparkContext {


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("WordCountApp").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    //    2) Read The text file
    val sourceRDD: RDD[String] = sparkContext.textFile("src/main/resources/word_count.txt")

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
