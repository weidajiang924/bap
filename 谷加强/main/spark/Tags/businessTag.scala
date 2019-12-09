package Tags


import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import util.{AmapUtil, NumFormat, StringUtil, jedisConnectionPool}

object businessTag extends Tag {

  override def addTag(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val long: Double = NumFormat.toDouble(row.getAs[String]("long"))
    val lat: Double = NumFormat.toDouble(row.getAs[String]("lat"))
    if(long>=73&&long<=135&&lat>=3&&lat<=54){
      val business = getBusiness(long,lat)
      if(StringUtil.isNotBlank(business)){
        val str = business.split(",")
        str.foreach(x=>{
          list:+=(x,1)
        })
      }
    }
    list
  }

  def getBusiness(long: Double, lat: Double):String = {
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    var business = jedisQuery(geohash)
    if(StringUtil.isNotBlank(business)){
      return business
    }else {
      business = AmapUtil.getBusiness(long,lat)
      if(StringUtil.isNotBlank(business)){
        jedisInsert(geohash,business)
      }
    }
    business
  }

  def jedisQuery(geohash: String) = {
    val jedis = jedisConnectionPool.getConnect()
    val business: String = jedis.get(geohash)
    jedis.close()
    business
  }

  def jedisInsert(geohash: String,business: String) = {
    val jedis = jedisConnectionPool.getConnect()
    jedis.set(geohash,business)
    jedis.close()
  }
}
