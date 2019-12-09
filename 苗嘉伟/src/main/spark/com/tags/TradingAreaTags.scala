package com.tags


import com.dmp_utils.{GeoHashUtil, TradingNameUtil}
import com.utils.GeoRedisForTrading

import scala.collection.mutable.ArrayBuffer


object TradingAreaTags {
  def main(args: Array[String]): Unit = {
    val latitude = 40.0691213379
    val longitude = 116.3526412354
    val geoHashStr: String = GeoHashUtil.getGeoHashUtils(longitude,latitude)
    val tradingAreaTags: ArrayBuffer[String] = TradingNameUtil.getNames(longitude,latitude)
    new GeoRedisForTrading().addRecord(geoHashStr,tradingAreaTags.toString())
  }
}
