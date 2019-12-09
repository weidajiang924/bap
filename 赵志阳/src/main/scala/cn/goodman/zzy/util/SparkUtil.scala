package cn.goodman.zzy.util

import cn.goodman.zzy.udf.DMPUdfUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * @Author : zzy
  * @Date : 2019/12/6
  * @Verson : 1.0
  */
object SparkUtil {
  val logger: Logger = LoggerFactory.getLogger(SparkUtil.getClass)

  //缓存级别
  var storageLevels: mutable.Map[String, StorageLevel] = mutable.Map[String, StorageLevel]()

  /**
    * 创建sparksession
    *
    * @param sconf
    * @return
    */
  def createSpark(sconf: SparkConf): SparkSession = {
    val spark: SparkSession = SparkSession.builder
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate();

    //加载自定义函数
    registerFun(spark)

    spark
  }

  def createSparkNotHive(sconf: SparkConf): SparkSession = {
    val spark: SparkSession = SparkSession.builder
      .config(sconf)
      .getOrCreate();

    //加载自定义函数
    registerFun(spark)

    spark
  }

  /**
    * udf注册
    *
    * @param spark
    */
  def registerFun(spark: SparkSession): Unit = {
    spark.udf.register("adSpaceTypeLabel",DMPUdfUtil.adSpaceTypeLabel _)
    spark.udf.register("adSpaceTypeNameLabel",DMPUdfUtil.adSpaceTypeNameLabel _)
    spark.udf.register("appNameLabel",DMPUdfUtil.appNameLabel _)
    spark.udf.register("adPlatfromLabel",DMPUdfUtil.adPlatfromLabel _)
    spark.udf.register("OSLabel",DMPUdfUtil.OSLabel _)
    spark.udf.register("networkLabel",DMPUdfUtil.networkLabel _)
    spark.udf.register("ispnameTable",DMPUdfUtil.ispnameTable _)
    spark.udf.register("keywordTable",DMPUdfUtil.keywordTable _)
  }

}
