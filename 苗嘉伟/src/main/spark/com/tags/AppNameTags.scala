package com.tags

import com.Traits.TagMarker
import org.apache.spark.sql.Row

object AppNameTags extends TagMarker{
  override def makeTags(row: Row): Map[String, Int] = {
    val data:String = row.getAs[String]("appname")
    Map("APP"+data -> 1)
  }
}
