package com.tags

import com.Traits.TagMarker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AreaTags extends TagMarker{
  override def makeTags(row: Row): Map[String, Int] = {
    val province:String = row.getAs[String]("provincename")
    val city:String = row.getAs[String]("cityname")
    val provinceTag:String = {
      if (StringUtils.isNotBlank(province)){
        "ZP"+province
      }
      else {
        ""
      }
    }
    val cityTag:String = {
      if (StringUtils.isNotBlank(city)){
        "ZC"+city
      }
      else ""
    }
    Map(provinceTag->1,cityTag->1)
  }
}
