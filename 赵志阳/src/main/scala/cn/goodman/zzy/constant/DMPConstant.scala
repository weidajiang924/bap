package cn.goodman.zzy.constant

/**
  * @Author : zzy
  * @Date : 2019/12/6
  * @Verson : 1.0
  */
object DMPConstant {
  // 字段名
  val SESSION_ID="sessionid"
  val AD_VERTISERS_ID="advertisersid"
  val AD_ORDER_ID="adorderid"
  val AD_CREATEIVE_ID = "adcreativeid"
  val PROVINCE_NAME="provincename"
  val CITY_NAME="cityname"

  //HbaseTableName
  val HTABLE_NAME="spark:DMPUserLabel"

  // 基础参数
  val SAVE_MODE="overwrite"
  val BASE_REPARTITION_NUM=1
}
