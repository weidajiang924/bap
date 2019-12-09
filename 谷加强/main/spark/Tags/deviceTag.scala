package Tags

import org.apache.spark.sql.Row

object deviceTag extends Tag {
  override def addTag(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val client: Int = row.getAs[Int]("client")
    val networkname: String = row.getAs[String]("networkmannername")
    val ispname: String = row.getAs[String]("ispname")
    client match {
      case 1 => list :+=("Android D00010001",1)
      case 2 => list :+=("IOS D00010002",1)
      case 3 => list :+=("WinPhone D00010003",1)
      case _ => list :+=("其 他 D00010004",1)
    }

    networkname match {
      case "WIFI" => list :+=("WIFI D00020001",1)
      case "4G" => list :+=("4G D00020002",1)
      case "3G" => list :+=("3G D00020003",1)
      case "2G" => list :+=("2G D00020004",1)
      case _ => list :+=("D00020005",1)
    }

    ispname match {
      case "移动" => list :+=("移动 D00030001",1)
      case "联通" => list :+=("联通 D00030002",1)
      case "电信" => list :+=("电信 D00030003",1)
      case _ => list :+=("D00030004",1)
    }
    list
  }
}
