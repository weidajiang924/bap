package com.dmp_data_product

import com.dmp_casebean.DmpSourceStructType
import com.dmp_utils.{SparkParams,SparkUtil}
import org.apache.spark.sql.{DataFrame,SparkSession}

object DmpDataCreate {
  val AppName: String = this.getClass.getName
  val spark: SparkSession = SparkUtil.StartSpark(AppName)
  def getSourceData:DataFrame = {
    //使用read.schema.csv读取csv文件
    val sourceData: DataFrame = spark.read.schema(DmpSourceStructType.getStructType).csv(SparkParams.DMP_LOG_PATH)
    sourceData
  }
  def getParquetSource: DataFrame = {
    val filePath = "./data/dmp_parquet"
    val data: DataFrame = spark.read.parquet(filePath)
    data
  }
  //将元数据存储为parquet格式
  def main(args: Array[String]): Unit = {
    val sourceData = getParquetSource
//    sourceData.select("isbid","iswin").groupBy("isbid","iswin").count().show()
    sourceData.show()
    //sourceData.write.format("parquet").save("./data/dmp_parquet")
  }
}
