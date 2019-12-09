package Tags

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import util._
import java.util

object tagsDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = sparkHelper.createSpark("tagsDemo")
    val sc: SparkContext = spark.sparkContext

    val day = "20191209"

    val appbc: Broadcast[Array[(String, String)]] = sc.broadcast(broadCast.getAppname(sc))
    val stopwordbc: Broadcast[Array[String]] = sc.broadcast(broadCast.getStopword(sc))

    val column = tagsColumnHelper.getTagsColumn
    val info: DataFrame = spark.read.parquet("D:\\data\\Output\\log_parquet\\")
      .filter(x=>StringUtil.isNotBlank(x.getAs[String]("uuid")))
      .selectExpr(column: _*)
    info.show(10)

    import spark.implicits._

    val tags: RDD[(String, List[(String, Int)])] = info.map(row => {
      val adtag = adTag.addTag(row)
      val apptag = appTag.addTag(row, appbc)
      val cntag = cnTag.addTag(row)
      val devicetag = deviceTag.addTag(row)
      val kwordtag = kwordTag.addTag(row, stopwordbc)
      val regiontag = regionTag.addTag(row)
      val businesstag = businessTag.addTag(row)
      val tags = adtag ++ apptag ++ cntag ++ devicetag ++ kwordtag ++ regiontag ++ businesstag
      val userid = row.getAs[String]("uuid")
      (userid, tags)
    }).rdd
    tags.take(10).foreach(println)
    val usertags: RDD[(String, List[(String, Int)])] = tags.reduceByKey(_ ++ _).map(x => {
      (x._1, x._2.groupBy(_._1).mapValues(_.map(_._2).sum).toList)
    })
    usertags.take(100).foreach(println)

    val tablename = "userTags"
    //HBaseUtil.createTable(tablename)
    val table: Table = HBaseUtil.getTable(tablename)

    val usertags1: RDD[(String, String)] = usertags.map(x => {
      val list = x._2
      val str = list.map(x =>  x._1 + ":" + x._2).mkString(",")
      (x._1, str)
    })
    var puts = new util.ArrayList[Put]()
    usertags1.collect().map(x=>{
      val put = new Put(Bytes.toBytes(x._1))
      put.addColumn(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(x._2))
      puts.add(put)
    })
    table.put(puts)
    HBaseUtil.closeTable(table)
    sc.stop()
    spark.stop()
  }
}
