package com.hackprotech

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PartitionBasics {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    // Create a configuration
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SparkBasics")
    sparkConf.setMaster("local[5]")

    //    local[*] or local[1] or local[4]

    //  SparkSession or SparkContext
    val sparkContext = new SparkContext(sparkConf)

    //  Create Distributed Datasets
    val sourceRDD = sparkContext.parallelize(Range(0, 20))

    println(s"Number of partitions in local ${sourceRDD.getNumPartitions}")

    //  Print it in the console
//    sourceRDD.foreach(println)
    sourceRDD.saveAsTextFile("src/main/resources/partitions")

    //    5 partitions
    //    => randomly assign the elements to the partitions
    //    => should see only 5 part files

    sparkContext.stop()

//     spark.sql.shuffle.partitions = 150


  }

}
