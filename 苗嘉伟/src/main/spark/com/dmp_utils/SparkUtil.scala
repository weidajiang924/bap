package com.dmp_utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtil {
  def StartSpark(AppName:String):SparkSession={
    SparkSession.builder.master("local[*]").appName(AppName).getOrCreate()
  }
  def StartSpark(configure:(String, String),AppName:String):SparkSession={
    val conf = new SparkConf().setAppName(AppName).setMaster("local[*]")
    conf.set(configure._1,configure._2)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark
  }

  def CloseSpark(spark:SparkSession): Unit ={
    spark.close()
  }
}
