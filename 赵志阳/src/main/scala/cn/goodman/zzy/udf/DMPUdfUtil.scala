package cn.goodman.zzy.udf

/**
  * @Author : zzy
  * @Date : 2019/12/7
  * @Verson : 1.0
  */
object DMPUdfUtil {

  /**
    * 广告位类型Id标签
    *
    * @param adType
    * @return
    */
  def adSpaceTypeLabel(adType: Int): Map[String, Int] = {
    var pre = "LC"
    if (adType < 10) {
      pre += "0"
    }
    pre += adType
    Map(pre -> 1)
  }


  /**
    * 广告位类型名称标签
    *
    * @param adType
    * @return
    */
  def adSpaceTypeNameLabel(adTypeName: String): Map[String, Int] = {
    Map("LN " + adTypeName -> 1)
  }

  /**
    * app名称标签
    *
    * @param appName
    * @return
    */
  def appNameLabel(appName: String): Map[String, Int] = {
    Map("APP " + appName -> 1)
  }

  /**
    * 渠道标签
    *
    * @param appName
    * @return
    */
  def adPlatfromLabel(adPlatfrom: String): Map[String, Int] = {
    Map("CN " + adPlatfrom -> 1)
  }

  /**
    * 操作系统标签
    *
    * @param client
    * @return
    */
  def OSLabel(client: Int): Map[String, Int] = {
    var pre: String = ""
    pre = client match {
      case 1 => "D00010001"
      case 2 => "D00010002"
      case 3 => "D00010003"
      case _ => "D00010004"
    }
    Map(pre -> 1)
  }

  /**
    * 联网方式标签
    *
    * @param networkmannername
    * @return
    */
  def networkLabel(networkmannername: String): Map[String, Int] = {
    var pre: String = ""
    pre = networkmannername.toUpperCase() match {
      case "WIFI" => "D00020001"
      case "4G" => "D00020002"
      case "3G" => "D00020003"
      case "2G" => "D00020004"
      case _ => "D00020005"
    }
    Map(pre -> 1)
  }

  /**
    * 运营商标签
    *
    * @param ispname
    * @return
    */
  def ispnameTable(ispname: String): Map[String, Int] = {
    var pre: String = ""
    pre = ispname match {
      case "移动" => "D00030001"
      case "联通" => "D00030002"
      case "电信" => "D00030003"
      case _ => "D00030004"
    }
    Map(pre -> 1)
  }

  /**
    * 关键字标签
    * @param kw
    * @return
    */
  def keywordTable(kw: String): Map[String, Int] = {
    val kws: Array[String] = kw.split("\\|")
    var kwMap:Map[String,Int]= Map()
    val maps: Array[Map[String, Int]] = kws.filter(x => x.length >= 3 & x.length <= 8)
      .map(x => {
        Map("K " + x -> 1)
      })
    for(map<-maps){
      kwMap=kwMap++map
    }
    kwMap
  }
}
