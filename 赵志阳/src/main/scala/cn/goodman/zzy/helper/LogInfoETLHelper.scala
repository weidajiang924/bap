package cn.goodman.zzy.helper

import cn.goodman.zzy.model.Log

import scala.collection.mutable.ArrayBuffer

/**
  * @Author : zzy
  * @Date : 2019/12/6
  * @Verson : 1.0
  */
object LogInfoETLHelper {
  /**
    * 为log日志添加Schema
    *
    * @return
    */
  def selectForAddSchema(): ArrayBuffer[String] = {
    val cols = new ArrayBuffer[String]()
    cols += ("_c0 as sessionid")
    cols += ("cast(_c1 as int) as advertisersid")
    cols += ("cast(_c2 as int) as adorderid")
    cols += ("cast(_c3 as int) as adcreativeid")
    cols += ("cast(_c4 as int) as adplatformproviderid")
    cols += ("_c5 as sdkversion")
    cols += ("_c6 as adplatformkey")
    cols += ("cast(_c7 as int) as putinmodeltype")
    cols += ("cast(_c8 as int) as requestmode")
    cols += ("cast(_c9 as double) as adprice")
    cols += ("cast(_c10 as double) as adppprice")
    cols += ("_c11 as requestdate")
    cols += ("_c12 as ip")
    cols += ("_c13 as appid")
    cols += ("_c14 as appname")
    cols += ("_c15 as uuid")
    cols += ("_c16 as device")
    cols += ("cast(_c17 as int) as client")
    cols += ("_c18 as osversion")
    cols += ("_c19 as density")
    cols += ("cast(_c20 as int) as pw")
    cols += ("cast(_c21 as int) as ph")
    cols += ("_c22 as longitude")
    cols += ("_c23 as latitude")
    cols += ("_c24 as provincename")
    cols += ("_c25 as cityname")
    cols += ("cast(_c26 as int) as ispid")
    cols += ("_c27 as ispname")
    cols += ("cast(_c28 as int) as networkmannerid")
    cols += ("_c29 as networkmannername")
    cols += ("cast(_c30 as int) as iseffective")
    cols += ("cast(_c31 as int) as isbilling")
    cols += ("cast(_c32 as int) as adspacetype")
    cols += ("_c33 as adspacetypename")
    cols += ("cast(_c34 as int) as devicetype")
    cols += ("cast(_c35 as int) as processnode")
    cols += ("cast(_c36 as int) as apptype")
    cols += ("_c37 as district")
    cols += ("cast(_c38 as int) as paymode")
    cols += ("cast(_c39 as int) as isbid")
    cols += ("cast(_c40 as double) as bidprice")
    cols += ("cast(_c41 as double) as winprice")
    cols += ("cast(_c42 as int) as iswin")
    cols += ("_c43 as cur")
    cols += ("cast(_c44 as double) as rate")
    cols += ("cast(_c45 as double) as cnywinprice")
    cols += ("_c46 as imei")
    cols += ("_c47 as mac")
    cols += ("_c48 as idfa")
    cols += ("_c49 as openudid")
    cols += ("_c50 as androidid")
    cols += ("_c51 as rtbprovince")
    cols += ("_c52 as rtbcity")
    cols += ("_c53 as rtbdistrict")
    cols += ("_c54 as rtbstreet")
    cols += ("_c55 as storeurl")
    cols += ("_c56 as realip")
    cols += ("cast(_c57 as int) as isqualityapp")
    cols += ("cast(_c58 as double) as bidfloor")
    cols += ("cast(_c59 as int) as aw")
    cols += ("cast(_c60 as int) as ah")
    cols += ("_c61 as imeimd5")
    cols += ("_c62 as macmd5")
    cols += ("_c63 as idfamd5")
    cols += ("_c64 as openudidmd5")
    cols += ("_c65 as androididmd5")
    cols += ("_c66 as imeisha1")
    cols += ("_c67 as macsha1")
    cols += ("_c68 as idfasha1")
    cols += ("_c69 as openudidsha1")
    cols += ("_c70 as androididsha1")
    cols += ("_c71 as uuidunknow")
    cols += ("_c72 as userid")
    cols += ("cast(_c73 as int) as iptype")
    cols += ("cast(_c74 as double) as initbidprice")
    cols += ("cast(_c75 as double) as adpayment")
    cols += ("cast(_c76 as double) as agentrate")
    cols += ("cast(_c77 as double) as lomarkrate")
    cols += ("cast(_c78 as double) as adxrate")
    cols += ("_c79 as title")
    cols += ("_c80 as keywords")
    cols += ("_c81 as tagid")
    cols += ("_c82 as callbackdate")
    cols += ("_c83 as channelid")
    cols += ("cast(_c84 as int) as mediatype")

    cols
  }

  /**
    * rdd 转Log对象
    *
    * @param words
    * @return
    */
  def handleLogETL(words: Array[String]): Log = {
    Log(words(0)
      , toInt(words(1))
      , toInt(words(2))
      , toInt(words(3))
      , toInt(words(4))
      , words(5)
      , words(6)
      , toInt(words(7))
      , toInt(words(8))
      , toDouble(words(9))
      , toDouble(words(10))
      , words(11)
      , words(12)
      , words(13)
      , words(14)
      , words(15)
      , words(16)
      , toInt(words(17))
      , words(18)
      , words(19)
      , toInt(words(20))
      , toInt(words(21))
      , words(22)
      , words(23)
      , words(24)
      , words(25)
      , toInt(words(26))
      , words(27)
      , toInt(words(28))
      , words(29)
      , toInt(words(30))
      , toInt(words(31))
      , toInt(words(32))
      , words(33)
      , toInt(words(34))
      , toInt(words(35))
      , toInt(words(36))
      , words(37)
      , toInt(words(38))
      , toInt(words(39))
      , toDouble(words(40))
      , toDouble(words(41))
      , toInt(words(42))
      , words(43)
      , toDouble(words(44))
      , toDouble(words(45))
      , words(46)
      , words(47)
      , words(48)
      , words(49)
      , words(50)
      , words(51)
      , words(52)
      , words(53)
      , words(54)
      , words(55)
      , words(56)
      , toInt(words(57))
      , toDouble(words(58))
      , toInt(words(59))
      , toInt(words(60))
      , words(61)
      , words(62)
      , words(63)
      , words(64)
      , words(65)
      , words(66)
      , words(67)
      , words(68)
      , words(69)
      , words(70)
      , words(71)
      , words(72)
      , toInt(words(73))
      , toDouble(words(74))
      , toDouble(words(75))
      , toDouble(words(76))
      , toDouble(words(77))
      , toDouble(words(78))
      , words(79)
      , words(80)
      , words(81)
      , words(82)
      , words(83)
      , toInt((words(84)))
    )
  }

  def toInt(words: String): Int = {
    if (words.nonEmpty) {
      words.trim.toInt
    } else {
      0
    }
  }

  def toDouble(words: String): Double = {
    if (words.nonEmpty) {
      words.trim.toDouble
    } else {
      0.0
    }
  }
}
