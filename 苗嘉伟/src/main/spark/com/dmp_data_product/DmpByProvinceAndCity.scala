package com.dmp_data_product

import com.dmp_casebean.DmpGroupByProvinceWithCity
import com.dmp_utils.{MYSQL_PROPERTIES, SparkParams}
import org.apache.spark.sql.{Dataset, SaveMode}

object DmpByProvinceAndCity {
  def getDmpByProvinceAndCity:Dataset[DmpGroupByProvinceWithCity] = {
    import DmpDataCreate.spark.implicits._
    val sourceData = DmpDataCreate.getSourceData
    //3.2.0	统计各省市数据量分布情况
    val dmpGroupByProvinceWithCity: Dataset[DmpGroupByProvinceWithCity] = sourceData.groupBy("provincename","cityname").count().map(
      perLine => {
        val provinceName = perLine.getString(0)
        val cityName = perLine.getString(1)
        val count = perLine.getLong(2)
        DmpGroupByProvinceWithCity(provinceName,cityName,count)
      })
    dmpGroupByProvinceWithCity
  }

  def main(args: Array[String]): Unit = {
    val dmpGroupByProvinceWithCity = DmpByProvinceAndCity.getDmpByProvinceAndCity

    //输出为json格式
    val path = "./data/dmpGroupByProvinceWithCity_json"
    dmpGroupByProvinceWithCity.write.mode(SaveMode.Overwrite).format("json").save(path)
    println("输出为json格式到磁盘完毕")

    //存储到mysql业务库中
    dmpGroupByProvinceWithCity.write.mode(SaveMode.Overwrite)
      .jdbc(SparkParams.MYSQL_CONNECT_URL, "dmpGroupByProvinceWithCity", MYSQL_PROPERTIES.GetConnectionProperties)
    println("输出到mysql数据库完毕")

    //存储到磁盘中
    dmpGroupByProvinceWithCity.write.mode(SaveMode.Overwrite).format("csv").save("./data/dmpGroupByProvinceWithCity_csv")
    println("输出到本地磁盘以csv格式完毕")
  }
}
