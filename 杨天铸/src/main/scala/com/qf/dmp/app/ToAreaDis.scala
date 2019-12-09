package com.qf.dmp.app

import com.qf.dmp.column.AllCol
import com.qf.dmp.util.{CallLabel, SparkUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ToAreaDis {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("ToAreaDis")
      .setMaster("local[*]")

    val spark: SparkSession = SparkUtil.getSession(conf)
    import spark.implicits._

    val broad: Broadcast[Array[String]] = spark.sparkContext.broadcast(
      spark.sparkContext.textFile("C:\\Users\\34439\\Desktop\\学习\\spark项目\\DMP项目资料\\DMP项目资料\\stopwords.txt")
        .collect()
    )
    //read parquet data
    spark.read.parquet("C:\\Users\\34439\\Desktop\\学习\\spark项目\\DMP\\src\\data\\output\\log\\part-00000-07cbbb6d-bf53-44f3-9112-70deb8c03486-c000.snappy.parquet")
      .selectExpr(AllCol.labelCol: _*)
      .map(x=>{
            val array: Array[String] = broad.value
          (CallLabel.labelAdv(x.get(0)),
            CallLabel.labelAdv(x.get(1)),
            CallLabel.labelKey(x.get(7).toString,array)
          )

        })

    spark.stop
  }

}
