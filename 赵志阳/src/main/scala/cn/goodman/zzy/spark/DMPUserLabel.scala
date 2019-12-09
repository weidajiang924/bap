package cn.goodman.zzy.spark

import java.util

import cn.goodman.zzy.constant.DMPConstant
import cn.goodman.zzy.helper.DMPColsHelper
import cn.goodman.zzy.model.Log
import cn.goodman.zzy.util.{HBaseUtil, SparkUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * 添加用户标签到HBase
  *
  * @Author : zzy
  * @Date : 2019/12/7
  * @Verson : 1.0
  */
object DMPUserLabel {
  val logger: Logger = LoggerFactory.getLogger(DMPUserLabel.getClass)

  /**
    * 或者userId
    *
    * @param row
    * @return
    */
  def getUserId(row: Row): String = {
    if (row.getString(0).nonEmpty && row.getString(0) != "未知" && row.getString(0).toUpperCase() != "NULL") return row.getString(0)
    if (row.getString(1).nonEmpty && row.getString(1) != "未知" && row.getString(1).toUpperCase() != "NULL") return row.getString(1)
    if (row.getString(2).nonEmpty) return row.getString(2)
    if (row.getString(3).nonEmpty) return row.getString(3)
    if (row.getString(4).nonEmpty) return row.getString(4)
    if (row.getString(5).nonEmpty) return row.getString(5)
    null
  }

  def handleUserLabel(spark: SparkSession, appName: String) = {
    val begin = System.currentTimeMillis()
    val saveMode = DMPConstant.SAVE_MODE
    val repartitionNum = DMPConstant.BASE_REPARTITION_NUM
    try {
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //0. 读取数据
      val logFrame = spark.read.parquet("dir/out/rdd_log2/*")
      val tablename: String = DMPConstant.HTABLE_NAME

      //1. 标签SQL
      val userLableCols: ArrayBuffer[String] = DMPColsHelper.selectUserLabelCols()

      val labelDF: DataFrame = logFrame
        //2. 帖标签
        .selectExpr(userLableCols: _*)

      //3. 合并标签
      val userLabelRDD: RDD[String] = labelDF.rdd.map(row => {
        val userid = getUserId(row)
        val adSpaceTypeLabel = row.getMap[String, Int](6)
        val adSpaceTypeNameLabel = row.getMap[String, Int](7)
        val appNameLabel = row.getMap[String, Int](8)
        val adPlatfromLabel = row.getMap[String, Int](9)
        val OSLabel = row.getMap[String, Int](10)
        val networkLabel = row.getMap[String, Int](11)
        val ispnameTable = row.getMap[String, Int](12)
        val keywordTable = row.getMap[String, Int](13)
        val labelMap: collection.Map[String, Int] =
          adSpaceTypeLabel ++ adSpaceTypeNameLabel ++ appNameLabel ++ adPlatfromLabel ++ OSLabel ++ networkLabel ++ ispnameTable ++ keywordTable
        (userid, labelMap.toList)
      }).filter(x => x._1.nonEmpty && x._1 != null)
        //聚合同一个用户的标签
        .groupByKey()
        .map(x => {
          //聚合后的标签在一个迭代器里面，先都放入一个集合里
          var kvList: List[(String, Int)] = Nil
          for (kv <- x._2) {
            kvList :::= kv
          }
          //将集合里的标签按着标签名聚合
          val labels: Map[String, Int] = kvList.groupBy(_._1).map(labelkv => {
            var cnt = 0
            for (v <- labelkv._2) {
              cnt += v._2
            }
            (labelkv._1, cnt)
          })
          //转成String后去掉默认自带的List()
          val label: String = labels.toList.toString
          x._1 + "|" + label.substring(5, label.size - 1)
        })

      //5. 存入hbase
      //数据读取到内存
      val userLabelCollect = userLabelRDD.collect
      //转变集合类型
      val userLabelList: util.List[String] = new util.ArrayList[String]
      userLabelCollect.foreach(x => userLabelList.add(x))
      //开始存储
      HBaseUtil.putData(tablename, userLabelList)
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

      handleUserLabel(spark, appName)


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
    val appName = "DMPUserLabel"
    handleJobs(appName)
  }
}
