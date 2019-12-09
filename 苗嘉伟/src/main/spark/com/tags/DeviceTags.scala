package com.tags

import com.Traits.TagMarker
import org.apache.spark.sql.Row

//添加设备标签
object DeviceTags extends TagMarker{
  //设备型号
  def OsTags(row:Row):Map[String,Int] = {
    val data:Int = row.getAs[Int]("client")
    data match {
      case 1 => Map("Android D00010001" -> 1)
      case 2 => Map("IOS D00010002" -> 1)
      case 3 => Map("Windows D00010003" -> 1)
      case _ => Map("其他 D00010004" -> 1)
    }
  }
  //网络型号
  def NetworkTags(row:Row):Map[String,Int] = {
    val data:String = row.getAs[String]("networkmannername")
    data match {
      case "WIFI" => Map("WIFI D00020001" -> 1)
      case "Wifi" => Map("WIFI D00020001" -> 1)
      case "4G" => Map("4G D00020002" -> 1)
      case "3G" => Map("3G D00020003" -> 1)
      case "2G" => Map("2G D00020004" -> 1)
      case _ => Map("其他 D00020005" -> 1)
    }
  }
  //运营商的型号

  //移 动 D00030001 联 通 D00030002 电 信 D00030003
  def IspTags(row:Row):Map[String,Int] = {
    val data:String = row.getAs[String]("ispname")
    data match {
      case "移动" => Map("移动 D00030001" -> 1)
      case "联通" => Map("联通 D00030001" -> 1)
      case "电信" => Map("电信 D00030002" -> 1)
      case _ => Map("其他 D00020005" -> 1)
    }
  }

  override def makeTags(row: Row): Map[String, Int] = {
    OsTags(row) ++ NetworkTags(row) ++ IspTags(row)
  }
}
