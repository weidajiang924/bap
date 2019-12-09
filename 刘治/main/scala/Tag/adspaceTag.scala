package Tag

import bean.tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object adspaceTag {
  def addTag(row:Row):List[(String,Int)]={
    var list = List[(String,Int)]()
    val adspacetype: Int = row.getAs("adspacetype").toString.toInt
    val adspacetypename = row.getAs("adspacetypename").toString
    adspacetype match{
      case value if value <10 => list:+= ("LC0"+adspacetype,1)
      case value if value >=10 => list:+= ("LC"+adspacetype,1)
    }
    if(adspacetypename!=""){
      list:+=("LN"+adspacetypename,1)
    }
    list
  }
}
