package cn.goodman.zzy.etl

import cn.goodman.zzy.constant.DMPConstant
import cn.goodman.zzy.helper.LogInfoETLHelper
import cn.goodman.zzy.model.Log
import cn.goodman.zzy.util.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * @Author : zzy
  * @Date : 2019/12/6
  * @Verson : 1.0
  */
object LogInfoETL {
  val logger: Logger = LoggerFactory.getLogger(LogInfoETL.getClass)


  def handleLogETL(spark: SparkSession, appName: String) = {
    val begin = System.currentTimeMillis()
    import spark.implicits._
    try {

      val saveMode=DMPConstant.SAVE_MODE
      // 1. SparkSession方式
//      val logFrame = spark.read.csv("dir/in/*")
//      val schemaColumns: ArrayBuffer[String] = LogInfoETLHelper.selectForAddSchema()
//      logFrame.selectExpr(schemaColumns: _*).as[Log]
//        .repartition(1)
//        .write.parquet("dir/out/log2/")

      // 2. SparkCore方式
            val sc: SparkContext = spark.sparkContext
            val logInfoRDD: RDD[String] = sc.textFile("dir/in/*")
            val logRdd: RDD[Log] = logInfoRDD.map(line => {
              line.split(",", -1)
            }).map(words => LogInfoETLHelper.handleLogETL(words))
            logRdd.toDF().repartition(1).write.mode(saveMode).parquet("dir/out/rdd_log2")

      //203274条记录


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

      handleLogETL(spark, appName)


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

    val appName = "LogInfoETL"
    handleJobs(appName)

  }

}
