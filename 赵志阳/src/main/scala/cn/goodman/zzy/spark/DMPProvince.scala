package cn.goodman.zzy.spark

import cn.goodman.zzy.constant.DMPConstant
import cn.goodman.zzy.model.Log
import cn.goodman.zzy.util.SparkUtil
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Author : zzy
  * @Date : 2019/12/6
  * @Verson : 1.0
  */
object DMPProvince {
  val logger: Logger = LoggerFactory.getLogger(DMPProvince.getClass)


  def handleProvince(spark: SparkSession, appName: String) = {
    val begin = System.currentTimeMillis()
    val saveMode = DMPConstant.SAVE_MODE
    val repartitionNum = DMPConstant.BASE_REPARTITION_NUM
    try {
      import spark.implicits._
      import org.apache.spark.sql.functions._
      val logFrame = spark.read.parquet("dir/out/rdd_log2/*")

      //1. 聚合条件
      val aggCondition: Seq[Column] = Seq[Column]($"${DMPConstant.PROVINCE_NAME}", $"${DMPConstant.CITY_NAME}")

      //2. 聚合查询结果
      val aggedDF: DataFrame = logFrame.groupBy(aggCondition: _*)
        .agg(
          count("*").as("ct")
        )
      aggedDF.show()

      //3. 落地为Json格式文件
      aggedDF.repartition(repartitionNum)
        .write.mode(saveMode)
        .json("dir/out/json_DMPProvince")


      //4. 存入Mysql数据库  表不存在会自动创建
      aggedDF.write
        .mode(saveMode)
        .format("jdbc")
        .option("url", "jdbc:mysql://hadoop100:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false")
        .option("dbtable", "test.DMPProvince")
        .option("user", "root")
        .option("password", "123456")
        .save()

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

      handleProvince(spark, appName)


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

    val appName = "DMPProvince"
    handleJobs(appName)

  }

}
