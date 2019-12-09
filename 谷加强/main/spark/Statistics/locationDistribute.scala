package Statistics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import util.{NumFormat, StatisticsColumnHelper, sparkHelper}

object locationDistribute {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = sparkHelper.createSpark("provinceCount")
    import spark.implicits._
    val column = StatisticsColumnHelper.getLocationDistribute_Column
    val df: DataFrame = spark.read.parquet("D:\\data\\Output\\log_parquet\\")
      .selectExpr(column:_*)
    df.show(10)
    val ld: RDD[String] = df.map(row => {
      val request = locationHandel.getRequest(row)
      val ad = locationHandel.getad(row)
      val show = locationHandel.getShow(row)
      val province = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")
      ((province, cityname), request ++ ad ++ show)
    }).rdd.reduceByKey((list1, list2) => {
      list1.zip(list2).map(l => l._1 + l._2)
    }).map(x => {
      x._1._1 + "," + x._1._2 + "," + x._2.mkString(",")
    })
    ld.map(line=>{
      val arr = line.split(",")
      val arr1 = arr(0)+arr(1)+arr(2)+arr(3)+arr(4)+arr(5)+arr(6)+(NumFormat.toDouble(arr(6)*100)/NumFormat.toDouble(arr(5))).toString+arr(7)+arr(8)+arr(9)+arr(10)
    })
    ld.take(10).foreach(println)
    spark.stop()
  }
}
