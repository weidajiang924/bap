package cn.goodman.zzy.spark

import cn.goodman.zzy.constant.DMPConstant
import cn.goodman.zzy.helper.DMPColsHelper
import cn.goodman.zzy.model.Log
import cn.goodman.zzy.util.SparkUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * @Author : zzy
  * @Date : 2019/12/6
  * @Verson : 1.0
  */
object DMPArealDistribution {

  val logger: Logger = LoggerFactory.getLogger(DMPArealDistribution.getClass)

  def handleArealDist(spark: SparkSession, appName: String) = {
    val begin = System.currentTimeMillis()
    val saveMode = DMPConstant.SAVE_MODE
    val repartitionNum = DMPConstant.BASE_REPARTITION_NUM
    try {
      import spark.implicits._
      import org.apache.spark.sql.functions._

      //0. 读取数据
      val logDF: DataFrame = spark.read.parquet("dir/out/rdd_log2/*")

      //1. 分组条件
      val groupConditions: Seq[Column] = Seq[Column]($"${DMPConstant.PROVINCE_NAME}", $"${DMPConstant.CITY_NAME}")
      //2. 要查询的列
      val fieldsConditions: ArrayBuffer[String] = DMPColsHelper.selectArealDistributionCols()
      //3. 生成表格数据
      logDF.selectExpr(fieldsConditions: _*)
        .groupBy(groupConditions: _*)
        .agg(
          sum(expr("CASE WHEN requestmode=1 and processnode>=1 THEN 1 ELSE 0 END"))
            .as("org_req_total"),
          sum(expr("CASE WHEN requestmode=1 and processnode>=2 THEN 1 ELSE 0 END"))
            .as("effect_req_total"),
          sum(expr("CASE WHEN requestmode=1 and processnode =3 THEN 1 ELSE 0 END"))
            .as("ad_req_total"),
          sum(expr("CASE WHEN iseffective=1 and isbilling =1 and isbid=1 THEN 1 ELSE 0 END"))
            .as("inrtb_cnt"),
          sum(expr("CASE WHEN iseffective=1 and isbilling =1 and iswin=1 and adorderid != 0 THEN 1 ELSE 0 END"))
            .as("rtbsuc_cnt"),
          sum(expr("CASE WHEN iseffective=1 and requestmode =2 THEN 1 ELSE 0 END"))
            .as("show_cnt"),
          sum(expr("CASE WHEN iseffective=1 and requestmode =3 THEN 1 ELSE 0 END"))
            .as("click_cnt")
        ).orderBy(groupConditions: _*).show()
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

      handleArealDist(spark, appName)


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

    val appName = "DMPArealDistribution"
    handleJobs(appName)

  }

}
