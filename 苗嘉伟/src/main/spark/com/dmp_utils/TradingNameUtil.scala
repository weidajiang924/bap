package com.dmp_utils

import com.alibaba.fastjson.JSON
import scalaj.http.{Http, HttpResponse}
import scala.collection.mutable.ArrayBuffer

object TradingNameUtil {
  def getNames(Longitude:Double,Latitude:Double):ArrayBuffer[String]={
    val key = "19741d341d31a6ee60f6969db3632c73"
    val url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+Longitude+","+Latitude+"&key="+key+"&radius=1000&extensions=all"
    val response: HttpResponse[String] = Http(url).asString
    val body = response.body
    val json = JSON.parseObject(body)
    val regeocode = json.getJSONObject("regeocode")
    val pois = regeocode.getJSONArray("pois")
    var arr:ArrayBuffer[String] = ArrayBuffer()
    for (i <- 0 until pois.size()){
      arr += pois.getJSONObject(i).get("name").toString
    }
    arr
  }
}
