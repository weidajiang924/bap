package Tags

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object broadCast {
  def getAppname(sc:SparkContext):Array[(String, String)]={
    val appArr: Array[(String, String)] = sc.textFile("D:\\data\\Input\\app_dict.txt").map(x => {
      var arr = x.split("\t")
      arr
    }).filter(arr => arr.length >= 5 && arr(3).startsWith("A"))
      .map(arr => {
        val appname = arr(1)
        val appid = arr(3)
        (appid, appname)
      }).collect()
    appArr
  }

  def getStopword(sc:SparkContext):Array[String]={
    val stopwordArr: Array[String] = sc.textFile("D:\\data\\Input\\stopwords.txt").collect()
    stopwordArr
  }
}
