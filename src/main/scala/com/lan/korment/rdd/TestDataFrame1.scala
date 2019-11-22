package com.lan.korment.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
/**
  * Classname TestDataFrame1
  * Description TODO
  * Date 2019/11/21 10:12
  * Created by LanKorment
  * RDD转换成为DataFrame
  */

object TestDataFrame1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)
    // 将本地的数据读入 RDD， 并将 RDD 与 case class 关联
    val peopleRDD = sc.textFile("C:\\Users\\LanKorment\\Desktop\\testdata.txt")
      .map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))
    import context.implicits._
    // 将RDD 转换成 DataFrames
    val df = peopleRDD.toDF
    //将DataFrames创建成一个临时的视图
    df.createOrReplaceTempView("people")
    //使用SQL语句进行查询
    context.sql("select * from people").show()
  }
  }