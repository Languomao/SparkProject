package com.lan.korment.pop_teacher_practice

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
/**
  * Classname SelfPartition
  * Description TODO
  * Date 2019/11/21 17:13
  * Created by LanKorment
  */

/**
  *1.自定义分区器
  *2.继承自Partitioner
  *3.subjects是一个字符串数组
  *
  * @param subjects
  */

class SelfPartition (subjects :Array[String]) extends Partitioner{
  /*当课程和分区之间没有定义规则时，需要自定义规则
  val rules = new mutable.HashMap[String ,Int]()
  var i = 0
   for (sub <- subjects){
     rules += (sub -> i)
     i+=1
   }
   */
  //直接固定map
  val rules = Map("bigdata"-> 1,"java"->2,"python"->3)//不用new 直接写Map

  //定义分区数    是个方法，而不是定义变量
  override def numPartitions: Int = {
    subjects.length
  }

  //获取具体分区
  override def getPartition(key: Any): Int ={
    val k = key.toString
    rules.getOrElse(k,0)
  }
}