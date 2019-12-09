package com.qf.dmp.util

import scala.collection.mutable

object CallLabel extends Serializable {

  //1)广告位类型（标签格式： LC03->1 或者 LC16->1）xx 为数字，
  // 小于 10 补 0，把广告位类型名称，LN 插屏->1
  def labelAdv(adspacetype: Any) = {
    adspacetype match {
      case a: String => Map("LN" + a -> 1)
      case b: Int => {
        if (b < 10) {
          Map("LC0" + adspacetype -> 1)
        } else {
          Map("LC" + adspacetype -> 1)
        }
      }
    }
  }

  //2)App 名称（标签格式： APPxxxx->1）xxxx 为 App 名称，
  // 使用缓存文件 appname_dict 进行名称转换；APP 爱奇艺->1
  //      （需要参考APP字典）
  def labelApp(str:String) ={
    if(str != null){
     Map("App"+str->1)
    }else{
      str
    }
  }

  //渠道（标签格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
  def labelAdp(str:String) ={
    if(str != null){
      Map("CN"+str->1)
    }else{
      str
    }
  }

  //5)关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
  //超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
  //(需要参考stopwords.txt）
  def labelKey(str:String,array:Array[String]) ={
    println(str)
    val data: Array[String] = str.split("\\|")
    var map= scala.collection.mutable.Map[String,Int]()
    for(item <- data){
        if(array.contains(item)||item.length<3||item.length>8){
          null
        }else{
          map += ("K"+item->1)
        }
      }
      map
  }


}
