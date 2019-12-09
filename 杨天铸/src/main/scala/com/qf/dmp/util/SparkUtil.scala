package com.qf.dmp.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object SparkUtil {

  private var instanceH: SparkSession = null

  private var instance: SparkSession = null

  //创建支持hive的SparkSession
  def getSessionHive(conf: SparkConf): SparkSession = {
    if (instanceH == null) {
      instanceH = SparkSession.builder()
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
    }
    instanceH
  }

  //创建普通SparkSession
  def getSession(conf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession.builder()
        .config(conf)
        .getOrCreate()
    }
    instance
  }


  //读取表中数据
  def readTableDate(spark:SparkSession,tableName:String): DataFrame ={
    val frame: DataFrame = spark.read.table(tableName)
    frame
  }

  //读取数据
  def readTableDate(spark:SparkSession,tableName:String,colNames:Seq[String]): DataFrame ={
    val frame: DataFrame = spark.read.table(tableName)
      .selectExpr(colNames: _*)
    frame
  }

  //有条件读取
  def readTableDate(spark:SparkSession,tableName:String,colNames:Seq[String],conditions:Column): DataFrame ={
    val frame: DataFrame = spark.read.table(tableName)
      .selectExpr(colNames: _*)
      .where(conditions)
    frame
  }

}
