package com.dmp_data_product

//根据地域（省，市）分布来计算各种指标
object DmpByArea {
  def main(args: Array[String]): Unit = {
    import DmpDataCreate.spark.implicits._
    import org.apache.spark.sql.functions._
    val sourceData = DmpDataCreate.getSourceData
    sourceData.where("isbid = 0").groupBy("provincename").agg(count("isbid")).show()
  }
}
