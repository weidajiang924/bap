package cn.goodman.zzy.helper

import scala.collection.mutable.ArrayBuffer

/**
  * @Author : zzy
  * @Date : 2019/12/6
  * @Verson : 1.0
  */
object DMPColsHelper {

  def selectArealDistributionCols(): ArrayBuffer[String] = {
    val cols = new ArrayBuffer[String]()
    cols += "requestmode" //请求方式
    cols += "adorderid" //广告id
    cols += "adprice" //广告价格
    cols += "processnode" //流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
    cols += "adpayment" //广告消费
    cols += "paymode" //针对平台商的支付模式，1：展示量投放(CPM) 2：点击
    cols += "iseffective" //有效标识
    cols += "isbilling" //是否收费
    cols += "isbid" //是否竞价
    cols += "bidprice" //竞价价格
    cols += "winprice" //竞价成功价格
    cols += "iswin" //是否成功
    cols += "provincename" //省
    cols += "cityname" //市
    cols
  }

  /**
    * 用户标签
    * @return
    */
  def selectUserLabelCols(): ArrayBuffer[String] = {
    val cols = new ArrayBuffer[String]()
    cols += "userid" //userID
    cols += "uuid" //设备唯一标识
    cols += "realip" //真实 ip
    cols += "imei" //imei
    cols += "mac" //mac
    cols += "device" //设备号
    cols += "adSpaceTypeLabel(adspacetype) as adSpaceTypeLabel" //广告位类型
    cols += "adSpaceTypeNameLabel(adspacetypename) as adSpaceTypeNameLabel" //广告位类型名称
    cols += "appNameLabel(appname) as appNameLabel" //app名称
    cols += "adPlatfromLabel(adplatformproviderid) as adPlatfromLabel" //渠道id
    cols += "OSLabel(client) as OSLabel" //操作系统 1：android 2：ios 3：wp
    cols += "networkLabel(networkmannername) as networkLabel" //联网方
    cols += "ispnameTable(ispname) as ispnameTable" //运营商
    cols += "keywordTable(keywords) as keywordTable" //关键字
    cols
  }
}
