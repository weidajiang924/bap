package com.xuebotao.businessprocess

import com.google.common.primitives.Bytes
import com.xuebotao.constant.Constant
import com.xuebotao.util.{CommercialUtil, RedisProcess, SparkUtil, VisitGaode}
import com.xuebotao.writer.PutTable
import com.xuebotao.writer.WriterToSave.levelJobs
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 商圈业务
  */
object Commercialbusiness {


  def main(args: Array[String]): Unit = {

    var spark: SparkSession = null
    //spark配置参数
    val sconf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.shuffle.partitions", "32")
      .set("hive.merge.mapfiles", "true")
      .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
      .set("spark.sql.crossJoin.enabled", "true")
      //.set("spark.sql.streaming.checkpointLocation",ShareCarConstant.SPARK_SSTREAMING_LOCUS_CHECKPOINT)
      .setAppName("savedata")
      .setMaster("local[4]")
    //创建sparksession对象不支持hive的
    spark = SparkUtil.createNOhiveSpark(sconf)

    //首先打标签处理业务
    commerciallevelJobs(spark)
    //1.保存数据到hbase
    // saveToHBase(spark)
    //2.保存数据到es

  }

  //商圈业务处理
  def commerciallevelJobs(spark:SparkSession): Unit ={
    val filedf = spark.read.parquet("src/main/parquet/20191206.parquet")
    //过滤出有唯一识别的
    // ip: String, uuid: String,device: String,imei: String,mac: String,
    //idfa: String,openudid: String,androidid: String

    val condition = Constant.filterRow()
    val columns = Constant.selectColumns2()
    //过滤出数据和保留userid，经度和纬度
    val linedf = filedf.selectExpr(columns: _*).where(condition)
    linedf.printSchema()
    linedf.show(100)
    val levelrdd: RDD[(String, List[(String, Int)])] = linedf.rdd.map(line => {
      var list = List[(String, Int)]()
      val userid = line.getAs[String]("userid")
      val longs = line.getAs[String]("long") //经度
      val lat = line.getAs[String]("lat") //维度
      if(longs==null || longs==0){
        (userid,list)
      }else {
        //判断是否是中国的
        if (!(longs.toDouble >= 73 && longs.toDouble <= 135 &&
          lat.toDouble >= 3 && lat.toDouble <= 54)) {
          (userid, list)
        }
        else {
          //1.先用经纬度得到geohash
          println(longs + lat)

          val hash = CommercialUtil.geohash(longs, lat)
          //2.再去redis查询
          val info = RedisProcess.selecthash(hash.toString)
          if (info.equals("null")) {
            //4.没有就请求高德，然后再放入redis
            val longsandlat = longs.toString + "," + lat.toString
            //这是所有的商圈
            val shangquan = VisitGaode.bussinessTagFromBaidu(longsandlat)
            //存入redis
            RedisProcess.savehash(hash.toString, shangquan);
            //打上标签
            val arr = shangquan.split(",")
            arr.foreach(t => {
              list :+= (t, 1)
            })
            (userid, list)
          }
          else {
            //3.有就查出来打标签
            val arr = info.split(",")
            arr.foreach(t => {
              list :+= (t, 1)
            })
            (userid, list)
          }
        }}})

      val levelreduce=levelrdd.reduceByKey((list1,list2)=>{
        (list1 ::: list2).groupBy(_._1)
          .mapValues(_.foldLeft(0)(_+_._2))
          .toList
       })
    //进行保存到hbase
    //1.首先收集结果
    val leveres=levelreduce.collect()
    println(leveres.toBuffer)
    //保存数据到hbase
    leveres.toBuffer.foreach(
      line=>{
        val value=line._2.map(t=>t._1+":"+t._2).mkString(",")
        PutTable.toSaveHbase(line._1,value)
      }
    )
  }

}
