package Tag

import bean.tag
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object keywordsTag {
  def addTag(row:Row):List[(String,Int)]={
    var list = List[(String,Int)]()
    val keywords = row.getAs("keywords").toString
    val arr = keywords.split("\\|")
     if(arr.length>=3&&arr.length<=8){
       for(a <- arr){
         list:+=(a,1)
       }
     }

    list
  }



}
