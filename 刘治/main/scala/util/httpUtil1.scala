package util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object httpUtil1 {
  def getString(url:String):String={
//    var list  = List[(String,Int)]
    val buffer = collection.mutable.ListBuffer[String]()
    val httpClient = HttpClients.createDefault()
    //val url:String = "https://restapi.amap.com/v3/geocode/regeo?key=bca4f504848523259afcba304c090ab9&location=116.310003,39.991957&output=json&extensions=all"
    val header:String = null
    val get = new HttpGet(url)    // 创建 get 实例

    val response = httpClient.execute(get) // 发送请求
    val res: String = EntityUtils.toString(response.getEntity,"UTF-8")
//    println(res)    // 获取

    val obj=JSON.parseObject(res)
    val regeocodeJson = obj.getJSONObject("regeocode")
    if(regeocodeJson == null) return null
    val addressComponent = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponent == null) return null
    val businessAreas = addressComponent.getJSONArray("businessAreas")
    if(businessAreas == null) return null
    // 循环处理json内的数组
    for (i<-businessAreas.toArray){
      if(i.isInstanceOf[JSONObject]){
        val json = i.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }
}
