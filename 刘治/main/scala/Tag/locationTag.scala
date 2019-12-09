package Tag

import bean.tag
import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object locationTag {

  val jedis: Jedis = util.jedisUtil.getJedisFromPool

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
