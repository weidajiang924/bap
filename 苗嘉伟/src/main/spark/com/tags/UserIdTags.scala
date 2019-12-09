package com.tags

import com.Traits.TagMarker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object UserIdTags extends TagMarker{
  override def makeTags(row: Row): Map[String, Int] = {
    val id_fields = "imei,mac,idfa,openudid,androidid,imeimd5,macmd5,idfamd5,openudidmd5,androididmd5,imeisha1,macsha1,idfasha1,openudidsha1,androididsha1"
    val ids: Array[String] = id_fields.split(",")
    var map:Map[String,String] = Map()
    for (i <- ids) {
      if (StringUtils.isNotBlank(row.getAs[String](i))) {
        map += (i -> row.getAs[String](i))
      }
    }
    val userid ={
      if(map.values.nonEmpty){
        map.values.toList.head
      }else{
        ""
      }
    }
    Map(userid ->1)
  }
}
