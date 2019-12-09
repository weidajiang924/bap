package util

import com.alibaba.fastjson.{JSON, JSONObject}



object AmapUtil {
  def getBusiness(long:Double,lat:Double):String={
    val location = long+","+lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?key=f709d56580c24518e9709735a889ecdb&location="+location
    val json = HttpUtil.get(url)
    val jsonObject = JSON.parseObject(json)
    val buffer = collection.mutable.ListBuffer[String]()
    val status = jsonObject.getIntValue("status")
    if(status==0)
      return null
    val regeocodeJson = jsonObject.getJSONObject("regeocode")
    if(regeocodeJson==null)
      return null
    val addressComponent = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponent == null)
      return null
    val businessAreas = addressComponent.getJSONArray("businessAreas")
    if(businessAreas == null)
      return null
    for(jo <- businessAreas.toArray()){
      if(jo.isInstanceOf[JSONObject]){
        val js = jo.asInstanceOf[JSONObject]
        buffer.append(js.getString("name"))
      }
    }
    buffer.mkString(",")
  }
}
