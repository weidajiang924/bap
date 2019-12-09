package util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object columnUtil {
  def keyColumn(row:Row):String = {
    var key = ""
    if(StringUtils.isNoneBlank(row.getAs[String]("imei"))) key+= "IM: "+row.getAs[String]("imei")
    if(StringUtils.isNoneBlank(row.getAs[String]("mac")))  key+= "MC: "+row.getAs[String]("mac")
    if(StringUtils.isNoneBlank(row.getAs[String]("idfa")))  key+= "ID: "+row.getAs[String]("idfa")
    if(StringUtils.isNoneBlank(row.getAs[String]("openudid"))) key+=  "OD: "+row.getAs[String]("openudid")
    if(StringUtils.isNoneBlank(row.getAs[String]("androidid"))) key+=  "AD: "+row.getAs[String]("androidid")
    if(StringUtils.isNoneBlank(row.getAs[String]("imeimd5")))  key+= "IMMD5: "+row.getAs[String]("imeimd5")
    if(StringUtils.isNoneBlank(row.getAs[String]("macmd5")))  key+= "MCMD5: "+row.getAs[String]("macmd5")
    if(StringUtils.isNoneBlank(row.getAs[String]("idfamd5")))  key+= "IDMD5: "+row.getAs[String]("idfamd5")
    if(StringUtils.isNoneBlank(row.getAs[String]("openudidmd5")))  key+= "ODMD5: "+row.getAs[String]("openudidmd5")
    if(StringUtils.isNoneBlank(row.getAs[String]("androididmd5"))) key+=  "ADMD5: "+row.getAs[String]("androididmd5")
    if(StringUtils.isNoneBlank(row.getAs[String]("imeisha1")))  key+= "IMS1: "+row.getAs[String]("imeisha1")
    if(StringUtils.isNoneBlank(row.getAs[String]("macsha1")))  key+= "MCS1: "+row.getAs[String]("macsha1")
    if(StringUtils.isNoneBlank(row.getAs[String]("idfasha1")))  key+= "IDS1: "+row.getAs[String]("idfasha1")
    if(StringUtils.isNoneBlank(row.getAs[String]("openudidsha1")))  key+= "ODS1: "+row.getAs[String]("openudidsha1")
    if(StringUtils.isNoneBlank(row.getAs[String]("androididsha1"))) key+=  "ADS1: "+row.getAs[String]("androididsha1")
    key
  }
}
