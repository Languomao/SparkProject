package com.lan.korment.rdd

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Classname TestDataFrame3
  * Description TODO
  * Date 2019/11/22 10:37
  * Created by LanKorment
  */
object TestDataFrame3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDataFrame3").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df: DataFrame = sqlContext.read.json("C:\\Users\\LanKorment\\Desktop\\testdata.txt")
    df.createOrReplaceTempView("people")
    sqlContext.sql("select * from people").show()
  }
}
