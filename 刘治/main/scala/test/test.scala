package test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object test {
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

    val APPRdd: RDD[String]= session.sparkContext.textFile("G:\\qianfeng\\DMPProject\\DMP\\src\\main\\resources\\app_dict.txt")
    val strings: Array[String] = APPRdd.collect()
    //    val BroadValue: Broadcast[Array[String]] = session.sparkContext.broadcast(strings)

    //    val keyArray: Array[(String, Row)] = FilterDS.select($"imei", $"mac", $"idfa", $"openudid", $"androidid", $"imeimd5", $"macmd5", $"idfamd5",
    //      $"openudidmd5", $"androididmd5", $"imeisha1", $"idfasha1", $"macsha1", $"openudidsha1", $"androididsha1").rdd.collect().map(row => {
    //      val key: String = row.getAs(0).toString + row.getAs(1).toString + row.getAs(2).toString + row.getAs(3).toString + row.getAs(4).toString + row.getAs(5).toString + row.getAs(6).toString + row.getAs(7).toString + row.getAs(8).toString + row.getAs(9).toString + row.getAs(10).toString + row.getAs(11).toString + row.getAs(12).toString + row.getAs(13).toString + row.getAs(14).toString
    //      (key, row)
    //    })

    val keyArray: Array[(String, Row)] = FilterDS.rdd.collect().map(row => {
      val key: String = util.columnUtil.keyColumn(row)
      (key, row)
    })

    //    val tuples: Array[(String, List[(String,Int)])] = keyArray.map(arr => {
    //
    ////      val tuples1: List[(String, Int)] =
    //    })
//
//    for(t <- tuples){
//
//      println(t._2.size)
//
//    }

  }
}
