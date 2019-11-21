package com.lan.korment.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/**
  * Classname TestDataFrame1
  * Description TODO
  * Date 2019/11/21 10:12
  * Created by LanKorment
  */

object TestDataFrame1 {
    def main (args: Array[String]){
      val logFile = "hdfs://10.11.6.91:9000/README.md"
      val conf = new SparkConf().setAppName("Simple Application")
      val sc = new SparkContext(conf)
      val logData = sc.textFile(logFile, 2).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    }
  }