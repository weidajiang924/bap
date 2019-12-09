package util

import scala.collection.mutable.ArrayBuffer

object StatisticsColumnHelper {
  def getLocationDistribute_Column:ArrayBuffer[String]={
    var column = new ArrayBuffer[String]()
    column.+=("sessionid")
    column.+=("requestmode")
    column.+=("processnode")
    column.+=("iseffective")
    column.+=("isbilling")
    column.+=("isbid")
    column.+=("iswin")
    column.+=("adorderid")
    column.+=("winprice")
    column.+=("adpayment")
    column.+=("provincename")
    column.+=("cityname")
    column
  }
}
