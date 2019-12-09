import dmp.common.Constant
import dmp.util.{AmapUtil, SparkUtils}
import org.apache.spark.sql.DataFrame

/**
  * Description:
  * Copyright (c),2019,JingxuanYan 
  * This program is protected by copyright laws. 
  *
  * @author 闫敬轩
  * @date 2019/12/6 20:22
  * @version 1.0
  */
object TestData {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession(false)
    val df: DataFrame = spark.read.parquet(Constant.LOG_SAVE_URL).persist()
    df.createOrReplaceTempView("tb_log")
    val regionDF = spark.sql(
      """
        |select
        |long_s,lat
        |from tb_log
        |where long_s > '0'
      """.stripMargin)
    regionDF.show(800000,false)
    val businame: String = AmapUtil.getBusinesss(119.5493219955203,35.4054054054054)
    println(businame)
    spark.stop()
  }
}
