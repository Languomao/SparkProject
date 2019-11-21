package com.lan.korment.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
  * Classname TopK
  * Description TODO
  * Date 2019/11/21 16:35
  * Created by LanKorment
  */

object TopK {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopK Key Words").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("C:\\Users\\LanKorment\\Desktop\\note.txt")
    val result= rdd1.flatMap(x=>x.split(" "))
      .filter(_.size>1)
      .map(x=>(x,1))
      .reduceByKey(_+_)
      .map{case(x,y)=>(y,x)}
      .sortByKey(false)
      .map{case(a,b)=>(b,a)}
    result.take(10).foreach(println)
  }
}
