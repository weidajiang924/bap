package com.qf.dmp.util

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

object AmapUtil {

  def getBusiness(longit:Double,lat:Double):String = {
    val location = longit + "," + lat

    val url = "https://restapi.amap.com/v3/geocode/regeo?key=ce39a7ad19d5a4a503f0267148d2a022&location="+location
    //调用地图 发送http请求
    val json: String = HttpUtil.get(url)
    val jsonObject: JSONObject = JSON.parseObject(json)
    val buffer: ListBuffer[String] = collection.mutable.ListBuffer[String]()

    //获取状态码
    val status: Int = jsonObject.getIntValue("status")
    if(status==0) return null

    //取值操作
    val regecodeJson: JSONObject = jsonObject.getJSONObject("regeocode")
    if(regecodeJson == null ) return null

    val addressComponent: JSONObject = regecodeJson.getJSONObject("addressComponent")
    if(addressComponent == null) return null

    val business: fastjson.JSONArray = addressComponent.getJSONArray("businessAreas")

    if(business == null) return null

    for(item<-business.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }

}
