package Tag

import  test._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object allTag {
  def main(args: Array[String]): Unit = {
    val spark: SparkConf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.testing.memory", "471859200")
    val session: SparkSession = SparkSession.builder().config(spark).getOrCreate()
    import session.implicits._
    val FilterDS: Dataset[Row] = session.read.parquet("G:\\qianfeng\\DMPProject\\DMP\\src\\main\\resources\\parquet")
      .filter("imei != '' or mac != '' or idfa != '' or openudid != '' or androidid != '' " +
        "or imeimd5 != '' or macmd5 != '' or idfamd5 != '' or openudidmd5 != '' or androididmd5 != '' or " +
        "imeisha1 != '' or macsha1 != '' or idfasha1 != '' or openudidsha1 != '' or androididsha1 != ''")

    val keyArray: Array[(String, Row)] = FilterDS.filter("longitude!='0' and latitude!='0'").limit(10).rdd.collect().map(row => {
      val key: String = util.columnUtil.keyColumn(row)
      (key, row)
    })

    val tuples: Array[(String, List[(String,Int)])] = keyArray.map(arr => {
      val tuples1: List[(String, Int)] = httpTest.addTag(arr._2)
      (arr._1,tuples1)
    })



  }
}
