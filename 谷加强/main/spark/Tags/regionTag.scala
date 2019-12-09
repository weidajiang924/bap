package Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import util.StringUtil

object regionTag extends Tag {
  override def addTag(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val province: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")
    if(StringUtil.isNotBlank(province))
      list:+=("ZP"+province,1)
    if(StringUtil.isNotBlank(city))
      list:+=("ZC"+city,1)
    list
  }
}
