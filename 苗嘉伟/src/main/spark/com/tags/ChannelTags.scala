package com.tags

import com.Traits.TagMarker
import org.apache.spark.sql.Row

object ChannelTags extends TagMarker{
  override def makeTags(row: Row): Map[String, Int] = {
    val data:Int = row.getAs[Int]("client")
    data match {
      case 1 => Map("Android" -> 1)
      case 2 => Map("IOS" -> 1)
      case 3 => Map("Windows" -> 1)
      case _ => Map("其他" -> 1)
    }
  }
}
