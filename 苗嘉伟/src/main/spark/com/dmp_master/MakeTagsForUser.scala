package com.dmp_master

import com.dmp_data_product.DmpDataCreate
import com.dmp_utils.SparkUtil
import com.tags.{AdPositionTags, AppNameTags, AreaTags, ChannelTags, DeviceTags, KeyWordsTags, UserIdTags}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MakeTagsForUser {
  def main(args: Array[String]): Unit = {
    val appName: String = "Label"
    val conf: (String, String) = {
      ("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }
    val spark: SparkSession = SparkUtil.StartSpark(conf, appName)
    val data: DataFrame = DmpDataCreate.getParquetSource
    //将parquet格式的元数据从磁盘中读出来
    //然后转为rdd将每行的信息遍历出来，使用spark core来处理
    data.rdd.map((perRow: Row) => {
      val Ad = AdPositionTags.makeTags(perRow)
      val AppName = AppNameTags.makeTags(perRow)
      val Channel = ChannelTags.makeTags(perRow)
      val Device = DeviceTags.makeTags(perRow)
      val KeyWords = KeyWordsTags.makeTags(perRow)
      val Area = AreaTags.makeTags(perRow)
      val UserId = UserIdTags.makeTags(perRow)
      (UserId.keys.head,Ad ++ AppName ++ Channel ++ Device ++ KeyWords ++ Area.toList)
    }).foreach(println)

    SparkUtil.CloseSpark(spark)
  }
}
