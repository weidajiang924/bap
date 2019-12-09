package Statistics

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import constant.MyConstan
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import _root_.util.sparkHelper

object provinceCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = sparkHelper.createSpark("provinceCount")

    import spark.implicits._
    val df: DataFrame = spark.read.parquet("D:\\data\\Output\\log_parquet\\")
      .selectExpr("sessionid", "provincename", "cityname")
    val rdd: RDD[Row] = df.rdd
    val result: DataFrame = rdd.map(row => {
      ((row(1), row(2)), 1)
    }).reduceByKey(_ + _).sortBy(-_._2).map(x => {
      ProvinceBean(x._2.toString, x._1._1.toString, x._1._2.toString)
    }).toDF
    result.write.json("D:\\data\\Output\\provinceCount\\")
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream(MyConstan.JDBC_PRO))
    properties.put("user",properties.getProperty(MyConstan.JDBC_USER))
    properties.put("password",properties.getProperty(MyConstan.JDBC_PWD))
    result.write.mode(SaveMode.Overwrite)
      .jdbc(properties.getProperty(MyConstan.JDBC_URL),properties.getProperty(MyConstan.JDBC_TABLENAME_PROVINCECOUNT),properties)
    spark.stop()
  }
}
case class ProvinceBean(ct:String,provincename:String,cityname:String)