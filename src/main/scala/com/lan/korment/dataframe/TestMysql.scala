package com.lan.korment.dataframe

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Classname TestMysql
  * Description TODO
  * Date 2019/11/22 10:41
  * Created by LanKorment
  */
object TestMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val url = "jdbc:mysql://10.1.1.7:3306/test"
    val table = "test1"
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","hadoop123")
    //需要传入Mysql的URL、表明、properties（连接数据库的用户名密码）
    val df = sqlContext.read.jdbc(url,table,properties)
    df.createOrReplaceTempView("test1")
    sqlContext.sql("select * from test1 limit 10").show()

  }
}
