package Tag

import bean.tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object appTag {
  def addTag(row:Row,arr:Array[String]):List[(String,Int)]={
    var list = List[(String,Int)]()
    val appid: String = row.getAs("appid").toString
    val appname = row.getAs("appname").toString
    list:+=("APP"+appname,1)
    list
  }
}
