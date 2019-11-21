package com.lan.korment.test

/**
  * Classname SparkPI
  * Description TODO
  * Date 2019/11/21 14:30
  * Created by LanKorment
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.random

object SparkPI {
  def main(args: Array[String]): Unit = {
    val masterUrl = "local[1]"
    val conf=new SparkConf().setMaster(masterUrl).setAppName("SparkPi")
    val sc=new SparkContext(conf)
    //启动Task数，默认2个
    val slices=if(args.length>0)args(0).toInt else 2
    // n是迭代次数（默认2w次）,Int.MaxValue是防止溢出
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    //默认两个patition,[1,100000]和[100001,20000]
    val count = sc.parallelize(1 until n, slices).map { i =>
      //产生的点范围[-1,1]，圆心是（0,0）
      val x = random * 2 - 1
      val y = random * 2 - 1
      //如果产生的点落在圆内计数1，否则计数0
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (n - 1))
  }
}
