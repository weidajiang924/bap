package Tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import util.StringUtil

object kwordTag extends Tag {
  override def addTag(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val keywords: String = row.getAs[String]("keywords")
    val stopwords = args(1).asInstanceOf[Broadcast[Array[(String)]]]
    val words: Array[String] = keywords.split("\\|").filter(x=>x.length>=3&&x.length<=8).distinct
    for (word <- words){
      if(StringUtil.isNotBlank(word) && !stopwords.value.contains(word))
        list:+=("K"+word,1)
    }
    list
  }
}
