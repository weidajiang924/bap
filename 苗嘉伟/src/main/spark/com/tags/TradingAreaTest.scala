package com.tags
import com.alibaba.fastjson.{JSON, JSONPObject}
import com.utils.GeoRedisForTrading
import scalaj.http.{Http, HttpResponse}

import scala.collection.mutable.ArrayBuffer

object TradingAreaTest {
  def main(args: Array[String]): Unit = {
    val key = "19741d341d31a6ee60f6969db3632c73"
    val Latitude = 40.0691213379
    val longitude = 116.3526412354
    val url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+longitude+","+Latitude+"&key="+key+"&radius=1000&extensions=all"
    val response: HttpResponse[String] = Http(url).asString
    val body = response.body
    val json = JSON.parseObject(body)
    val regeocode = json.getJSONObject("regeocode")
    val pois = regeocode.getJSONArray("pois")
    var arr:ArrayBuffer[String] = ArrayBuffer()
    for (i <- 0 until pois.size()){
      arr += pois.getJSONObject(i).get("name").toString
    }
    arr.foreach(println)
  }
}
