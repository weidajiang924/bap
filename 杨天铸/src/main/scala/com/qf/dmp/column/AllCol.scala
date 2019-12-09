package com.qf.dmp.column

import scala.collection.mutable.ArrayBuffer

object AllCol {
  //label
  def labelCol():ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]
//    columns += "appid"
    columns += "adspacetype"
    columns += "adspacetypename"
    columns += "appname"
    columns += "adplatformproviderid"
    columns += "client"
    columns += "networkmannername"
    columns += "ispname"
    columns += "keywords"
    columns += "provincename"
    columns += "cityname"
    columns
  }

}
