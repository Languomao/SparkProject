package com.lan.korment.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
  * Classname AvgAge
  * Description TODO
  * Date 2019/11/21 16:20
  * Created by LanKorment
  */

object AvgAge {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("Spark Exercise:Average Age Calculator").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("C:\\Users\\LanKorment\\Desktop\\age.txt",5);
    val count = rdd.count()
    val totalAge =rdd.map(line => line.split("\t")(1))
      .map(age => Integer.parseInt(String.valueOf(age)))
      .collect()
      .reduce(_+_)
    println("Total Age:" + totalAge + ";Number of People:" + count )
    val avgAge : Double = totalAge.toDouble / count.toDouble
    println("Average Age is " + avgAge)
  }
}
