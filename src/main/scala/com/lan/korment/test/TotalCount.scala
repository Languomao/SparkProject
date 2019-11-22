package com.lan.korment.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Classname TotalCount
  * Description TODO
  * Date 2019/11/22 14:28
  * Created by LanKorment
  */
object TotalCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("TotalCont")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("C:\\Users\\LanKorment\\Desktop\\age.txt",5)

    val count = rdd.count()
    //println("一共有"+ count + "行记录")

    val TotalAge = rdd.map(line => line.split("\t")(1))
      .map(age => Integer.parseInt(age)).collect().reduce(_+_)
    println(TotalAge)

  }

}
