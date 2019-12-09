package util

import scala.collection.mutable.ArrayBuffer

object tagsColumnHelper {
  def getTagsColumn:ArrayBuffer[String]={
    var column = new ArrayBuffer[String]()
    column.+=("adspacetype")
    column.+=("adspacetypename")
    column.+=("appname")
    column.+=("appid")
    column.+=("adplatformproviderid")
    column.+=("client")
    column.+=("networkmannername")
    column.+=("ispname")
    column.+=("keywords")
    column.+=("provincename")
    column.+=("cityname")
    column.+=("long")
    column.+=("lat")
    column.+=("uuid")
    /*column.+=("imei")
    column.+=("mac")
    column.+=("idfa")
    column.+=("openudid")
    column.+=("androidid")
    column.+=("imeimd5")
    column.+=("macmd5")
    column.+=("idfamd5")
    column.+=("openudidmd5")
    column.+=("androididmd5")
    column.+=("imeisha1")
    column.+=("macsha1")
    column.+=("idfasha1")
    column.+=("openudidsha1")
    column.+=("androididsha1")*/
    column
  }

}
