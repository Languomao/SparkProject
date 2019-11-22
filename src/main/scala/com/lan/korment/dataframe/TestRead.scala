package com.lan.korment.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Classname TestRead
  * Description TODO
  * Date 2019/11/22 10:39
  * Created by LanKorment
  */
object TestRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestRead").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //方式一
    val df1 = sqlContext.read.json("E:\\666\\people.json")
    val df2 = sqlContext.read.parquet("E:\\666\\users.parquet")
    //方式二
    val df3 = sqlContext.read.format("json").load("E:\\666\\people.json")
    val df4 = sqlContext.read.format("parquet").load("E:\\666\\users.parquet")
    //方式三，默认是parquet格式
    val df5 = sqlContext.load("E:\\666\\users.parquet")
  }
}
