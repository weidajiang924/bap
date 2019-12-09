package com.tags

import com.Traits.TagMarker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

object KeyWordsTags extends TagMarker{
  //加上关键字的标签
  override def makeTags(row: Row): Map[String, Int] = {
    val data:String = row.getAs[String]("keywords")
    val list = SplitKeywords(data).toList.toString
    Map(list ->1)
  }

  //控制关键字在8个以内
  //关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
  def SplitKeywords(keywords:String):Array[String] = {
    if (StringUtils.isNotBlank(keywords)){
      if (keywords.contains("|")){
        val arr: Array[String] = keywords.split("\\|")
        val arr1 = new ArrayBuffer[String]()
        val length = {
          if (arr.length>8){
            8
          }else{
            arr.length
          }
        }
        for(i <- 0 until length){
          arr1 += arr(i)
        }
        arr1.toArray
      }
      else {
        Array[String](keywords)
      }
    }else{
      Array[String]()
    }
  }
}
