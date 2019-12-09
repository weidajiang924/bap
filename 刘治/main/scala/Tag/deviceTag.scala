package Tag

import bean.tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object deviceTag {
  def addTag(row:Row):List[(String,Int)]={
    var list =  List[(String,Int)]()
    val client: Int = row.getAs("client").toString.toInt
    val networkmannername = row.getAs("networkmannername").toString
    val ispname = row.getAs("ispname").toString
    client match{
      case value if value == 1 => list:+= ("Android D00010001",1)
      case value if value == 2 => list:+= ("IOS D00010002",1)
      case value if value == 3 => list:+= ("WinPhone D00010003",1)
      case _ => list:+= ("其 他 D00010004",1)
    }
    networkmannername match {
      case value if value == "WIFI" => list:+= ("WIFI D00020001 ",1)
      case value if value == "4G" => list:+= ("4G D00020002",1)
      case value if value == "3G" => list:+= ("3G D00020003",1)
      case value if value == "2G" => list:+= ("2G D00020004",1)
      case _ => list:+= ("_   D00020005",1)
    }
    ispname match {
      case value if value == "移 动" => list:+= ("移 动 D00030001",1)
      case value if value == "联 通" => list:+= ("联 通 D00030002",1)
      case value if value == "电 信" => list:+= ("电 信 D00030003",1)
      case _ => list:+= ("_ D00030004",1)
    }

    list
  }

}
