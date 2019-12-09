package Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object appTag extends Tag {
  override def addTag(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val appbc: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String,String]]]
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")
    if(appname!=null && !appname.isEmpty)
      list:+=("APP"+appname,1)
    else if(appid!=null && !appid.isEmpty ){
      val appstr: String = appbc.value.getOrElse(appid,appid)
      list:+=("APP"+appstr,1)
    }
    list
  }
}
