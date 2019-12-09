package com.qf.dmp.app

import ch.hsr.geohash.GeoHash
import com.qf.dmp.util.{AmapUtil, JedisImpl, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

object ToProduct {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")

    val spark: SparkSession = SparkUtil.getSession(conf)
    import spark.implicits._
    val df: DataFrame = spark.read.parquet("C:\\Users\\34439\\Desktop\\学习\\spark项目\\DMP\\src\\data\\output\\log\\part-00000-2c42263c-76d4-4d5b-843e-5d79f66ea8e5-c000.snappy.parquet")

    df.select("longitude","lat").filter(row=>{
      bolfun(row.get(0).toString,row.get(1).toString)
    }).map(row=>{
      val lat = row.getAs[String]("lat").toDouble
      val longit = row.getAs[String]("longitude").toDouble
      val geoHash = GeoHash.withCharacterPrecision(lat,longit,6).toBase32
      println(geoHash.toString)
      var busniess = redisQuery(geoHash)
      println("start1==="+busniess)
      if(busniess == null || busniess.length == 0){
        busniess = AmapUtil.getBusiness(longit,lat)
        println("start2==="+busniess)
        if(busniess !=null&&busniess.length>0){
          //插入redis
          redisSet(geoHash,busniess)
        }
      }
      var list = List[(String,Int)]()
      if(StringUtils.isNoneBlank(busniess)) {
        val str = busniess.split(",")
        str.foreach(t => {
          list :+= (t, 1)
        })
      }
      list
    }).filter(_.nonEmpty)


    spark.stop()
  }


  def bolfun(x:String,y:String): Boolean ={
    if((x.equals("")||x.length==0)&&(y.equals("")||y.length==0)){
      return false
    }
    if(x == null || y== null){
      return false
    }
    if(x.equals("0")&&y.equals("0")){
      return false
    }
    return true
  }

  //通过redis数据库获取商圈
  def redisQuery(str:String): String ={
    val jedis:Jedis = JedisImpl.getJedisPool()
    val business: String = jedis.get(str)
    JedisImpl.close(jedis)
    business
  }

  //向redis插入数据
  def redisSet(geoHash:String,business:String): Unit ={
    val jedis: Jedis = JedisImpl.getJedisPool()
    println(business)
    jedis.set(geoHash,business)
    JedisImpl.close(jedis)
  }
}
