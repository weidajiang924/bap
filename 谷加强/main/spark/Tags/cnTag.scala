package Tags

import org.apache.spark.sql.Row

object cnTag extends Tag {
  override def addTag(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val adderid: String = row.getAs[Int]("adplatformproviderid").toString
    if(adderid!=null && !adderid.isEmpty)
      list:+=("CN"+adderid,1)
    list
  }
}
