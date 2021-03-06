package com.lan.korment.pop_teacher_practice

/**
  * Classname FavoriteTeacher
  * Description TODO
  * Date 2019/11/21 17:16
  * Created by LanKorment
  */
import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 1.访问记录存储是一个URL，暂时用一个records = Array[String]来存储
  * 2.将records转换成text(一个rdd)
  * 3.对text进行操作，如：mapPartitions，map
  * 4.将操作后的结果收集并写出到控制台
  */


object FavoriteTeacher{
  def main (args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("FavoriteTeacher").setMaster("local")
    val sc = new SparkContext(conf)

    //存储文本
    val records: Array[String] = Array("http://bigdata.xiaoniu.com/laozhao",
      "http://bigdata.xiaoniu.com/laozhao",
      "http://bigdata.xiaoniu.com/laozhao",
      "http://bigdata.xiaoniu.com/laozhao",
      "http://bigdata.xiaoniu.com/laozhao",
      "http://java.xiaoniu.com/laozhang",
      "http://java.xiaoniu.com/laozhang",
      "http://python.xiaoniu.com/laoqian",
      "http://java.xiaoniu.com/laoli",
      "http://python.xiaoniu.com/laoli",
      "http://python.xiaoniu.com/laoli")
    val text: RDD[String] = sc.parallelize(records)//转换成rdd
    print("First disposition:")
    text.collect().foreach(println)
    //打印结果如下：http://bigdata.xiaoniu.com/laozhao
    //    http://bigdata.xiaoniu.com/laozhao
    //    http://bigdata.xiaoniu.com/laozhao
    //    http://bigdata.xiaoniu.com/laozhao
    //    http://bigdata.xiaoniu.com/laozhao
    //    http://java.xiaoniu.com/laozhang
    //    http://java.xiaoniu.com/laozhang
    //    http://python.xiaoniu.com/laoqian
    //    http://java.xiaoniu.com/laoli
    //    http://python.xiaoniu.com/laoli
    //    http://python.xiaoniu.com/laoli

    /*
      1.处理lines,并返回一个(String,String)元组
     */
    def fun1(lines :String ): (String, String) = {
      val url = new URL(lines)//将lines转换成URL
      val hostName = url.getHost//获取host
      val path = url.getPath//获取path
      val courseName = hostName.substring(0,hostName.indexOf("."))//获取课程名
      val teacherName = path.substring(1)//获取教师的姓名
      (courseName,teacherName)
    }
    val res1: RDD[(String, String)] = text.map(fun1)
    print("Second disposition:")
    res1.foreach(print)
    //打印结果如下：(bigdata,laozhao)(bigdata,laozhao)(bigdata,laozhao)
    // (bigdata,laozhao)(bigdata,laozhao)(java,laozhang)(java,laozhang)(python,laoqian)
    // (java,laoli)(python,laoli)(python,laoli)


    val res2: RDD[((String, String), Int)] = res1.map(x => (x,1))//形成一个map 组合
    val res3: RDD[((String, String), Int)] = res2.reduceByKey(_+_)//根据Key将每个map合并
    print("Third disposition:")
    res3.foreach(print)
    val res4: RDD[(String, Iterable[((String, String), Int)])] = res3.groupBy(_._1._1)//根据学科来分组
    res4.foreach(println)
    val finRes  = res4.mapValues(x => x.toList.sortBy(_._2).reverse.take(2))//对value操作！很重要
    finRes.foreach(print)

    //    val selfPartition = new SelfPartition(records)//new 一个分区对象
    //    val res4 = res2.reduceByKey(selfPartition,_+_)
  }
}
