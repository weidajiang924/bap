package cn.goodman.zzy.spark

import cn.goodman.zzy.constant.DMPConstant
import cn.goodman.zzy.model.Log
import cn.goodman.zzy.util.SparkUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  * 商圈指标
  *
  * @Author : zzy
  * @Date : 2019/12/9
  * @Verson : 1.0
  */
object DMPBusinessArea {
  val logger: Logger = LoggerFactory.getLogger(DMPBusinessArea.getClass)


  def handleBA(spark: SparkSession, appName: String) = {
    val begin = System.currentTimeMillis()
    try {
      import spark.implicits._
      import org.apache.spark.sql.functions._



    } catch {
      case ex: Exception => {
        println(s"SyncDataJob.handleSyncData occur exception：app=[$appName], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    } finally {
      println(s"SyncDataJob.handleSyncData End：appName=[${appName}], use=[${System.currentTimeMillis() - begin}ms]")
    }

  }

  def handleJobs(appName: String) = {
    var spark: SparkSession = null
    try {
      //spark配置参数
      val sconf = new SparkConf()
        .set("spark.sql.shuffle.partitions", "32")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .setAppName(appName)
        .setMaster("local[4]")
      sconf.registerKryoClasses(Array(classOf[Log]))

      //spark上下文会话
      spark = SparkUtil.createSparkNotHive(sconf)

      handleBA(spark, appName)


    } catch {
      case ex: Exception => {
        println(s"SyncDataJob.handleJobs occur exception：app=[$appName], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //    val Array(appName) = args

    val appName = "DMPBusinessArea"
    handleJobs(appName)

  }
}
