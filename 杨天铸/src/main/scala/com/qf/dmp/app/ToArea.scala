package com.qf.dmp.app

import com.qf.dmp.util.{AreaUtil, SparkUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ToArea {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")

    val spark: SparkSession = SparkUtil.getSession(conf)
    import spark.implicits._
    val df: DataFrame = spark.read.parquet("C:\\Users\\34439\\Desktop\\学习\\spark项目\\DMP\\src\\data\\output\\log\\part-00000-07cbbb6d-bf53-44f3-9112-70deb8c03486-c000.snappy.parquet")
    val rdd: RDD[((String, String), List[Double])] = df.rdd.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffstive = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //统计符合条件的数据
      val reqList = AreaUtil.request(requestmode, processnode)
      val bidList = AreaUtil.dsp(iseffstive, isbilling, isbid, iswin, adorderid,
        winPrice, adpayment
      )
      val showList = AreaUtil.chick(requestmode, iseffstive)
      val province = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      ((province, city), reqList ++ bidList ++ showList)
    })
    //(1,0,1,0)
    rdd.reduceByKey((x,y)=>{
      x.zip(y).map(tup=>{
        tup._1+tup._2
      })
    }).map(tup=>tup._1._1+":"+tup._1._2+":"+tup._2.mkString(","))
      .saveAsTextFile("src/data/output/area")

    spark.stop()
  }

}
