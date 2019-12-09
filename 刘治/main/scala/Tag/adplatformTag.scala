package Tag

import bean.tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object adplatformTag {
  def addTag(row:Row):List[(String,Int)]={
    var list = List[(String,Int)]()
    val adplatformproviderid: Int = row.getAs("adplatformproviderid").toString.toInt
    list:+=("CN"+adplatformproviderid,1)
    list
  }
}
