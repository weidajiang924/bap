package Tags

import org.apache.spark.sql.Row

object adTag extends Tag {
  override def addTag(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val adtype: Int = row.getAs[Int]("adspacetype")
    val adTypeName: String = row.getAs[String]("adspacetypename")
    adtype match {
      case value if value < 10 => list :+=("LC0"+value,1)
      case value if value >= 10 => list :+=("LC"+value,1)
    }
    if(adTypeName != null && !adTypeName.isEmpty){
      list :+= ("LC"+adTypeName,1)
    }
    list
  }
}
