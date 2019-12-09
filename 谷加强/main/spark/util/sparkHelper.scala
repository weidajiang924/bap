package util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object sparkHelper {
  def createSparkContext(appName:String):SparkContext={
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .setAppName(appName)
      .setMaster("local[*]")
      //spark2.0之后，默认的压缩方式就是snappy
      .set("spark.sql.parquet.compression.codec","snappy")
    val sc = new SparkContext(conf)
    sc
  }

  def createSpark(appName:String):SparkSession={
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .setAppName(appName)
      .setMaster("local[*]")
      //spark2.0之后，默认的压缩方式就是snappy
      .set("spark.sql.parquet.compression.codec","snappy")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark
  }
}
