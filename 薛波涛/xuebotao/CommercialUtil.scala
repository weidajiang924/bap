package com.xuebotao.util

import ch.hsr.geohash.GeoHash

/**
  * 商圈的工具类
  */
object CommercialUtil extends Serializable {


  //利用经纬度获得geohasn编码
  def geohash(longs:String , lat:String): GeoHash ={
    val hash: GeoHash = GeoHash.withCharacterPrecision(lat.toDouble,longs.toDouble,8)
    hash
  }

}
