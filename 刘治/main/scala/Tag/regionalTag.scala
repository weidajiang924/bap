package Tag

import bean.tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object regionalTag {
  def addTag(row:Row):List[(String,Int)]= {
    var list = List[(String, Int)]()
    val provincename = row.getAs("provincename").toString
    val cityname = row.getAs("cityname").toString

    if (cityname != "") {
      list :+= ("ZC" + cityname, 1)
    }

    if (provincename != "") {
      list :+= ("ZP" + provincename, 1)
    }
    list
  }
}
