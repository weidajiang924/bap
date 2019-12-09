package test


import util.TypeUtil

import Tag.locationTag.jedis
import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row

object httpTest {
  def main(args: Array[String]): Unit = {
    val httpClient = HttpClients.createDefault()
    val url:String = "https://restapi.amap.com/v3/geocode/regeo?key=bca4f504848523259afcba304c090ab9&location=116.310003,39.991957&output=json&extensions=all"
    val header:String = null
    val get = new HttpGet(url)    // 创建 get 实例

    val response = httpClient.execute(get) // 发送请求
    val res: String = EntityUtils.toString(response.getEntity,"UTF-8")
    println(res)    // 获取

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
       println(json.getString("name"))
      }
    }

  }

  def addTag(row:Row):List[(String,Int)]={
    var list = List[(String,Int)]()
    val longitude: Double = util.TypeUtil.toDouble(row.getAs[String]("longitude") )
    val latitude: Double = util.TypeUtil.toDouble(row.getAs[String]("latitude") )
    val hash: GeoHash = GeoHash.withCharacterPrecision( latitude,longitude,6)
    val base = hash.toBase32
    if(jedis.exists(base)){
      list:+=(jedis.get(base),1)
    }
    else{
      val url = "https://restapi.amap.com/v3/geocode/regeo?key=bca4f504848523259afcba304c090ab9&output=json&extensions=all&location="+longitude+","+latitude
      val respnse: String = util.httpUtil1.getString(url)
      if(respnse!=null){
        val strs = respnse.split(",")
        for(str <- strs){
          list:+=(str,1)
        }
        println(base +"    "+respnse)
        jedis.set(base,respnse)
      }
    }
    list
  }
}
