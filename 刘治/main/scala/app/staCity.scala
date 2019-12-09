package app

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.count
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import util.TypeUtil

import scala.collection.mutable.ListBuffer

object staCity {
  def main(args: Array[String]): Unit = {
    val spark: SparkConf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.testing.memory", "471859200")
    val session: SparkSession = SparkSession.builder().config(spark).getOrCreate()
    import session.implicits._
    val frame: DataFrame = session.read.parquet("G:\\qianfeng\\DMPProject\\DMP\\src\\main\\resources\\a.txt\\part-00000-492db9db-bac4-4b62-b907-e7c01c0d5a26-c000.snappy.parquet")

    val frame1: DataFrame = frame.groupBy($"provincename", $"cityname").agg(
      count($"sessionid").alias("count")
    )
    val ds: Dataset[CityCount] = frame1.map(x => {
      CityCount(TypeUtil.toInt(x(2).toString), x(0).toString, x(1).toString)
    })
    frame1.coalesce(1).write.json("G:\\qianfeng\\DMPProject\\DMP\\src\\main\\resources\\json")




    session.stop()
  }

  case class CityCount(count:Int,provincename:String,cityname:String)
}


