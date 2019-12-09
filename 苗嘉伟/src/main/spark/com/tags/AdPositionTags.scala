package com.tags

import com.Traits.TagMarker
import org.apache.spark.sql.Row

object AdPositionTags extends TagMarker{
  override def makeTags(row: Row): Map[String, Int] = {
    val data: Int = row.getAs[Int]("adspacetype")
    data match {
      case x if x < 10 => Map("LC0"+x -> 1)
      case x if x > 9 =>  Map("LC"+x -> 1)
    }
  }
}
