package com.Traits

import org.apache.spark.sql.Row
//添加标签的特质
trait TagMarker {
  def makeTags(row:Row):Map[String,Int]
}
