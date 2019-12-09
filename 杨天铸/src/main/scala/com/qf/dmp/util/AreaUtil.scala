package com.qf.dmp.util

import scala.collection.mutable.ListBuffer

object AreaUtil {

  //原始请求、有效请求、广告请求
  def request(requestmode: Int, processnode: Int): List[Double] = {
    if (requestmode == 1 && processnode >= 1) {
      List(1, 0, 0)
    } else if (requestmode == 1 && processnode >= 2) {
      List(1, 1, 0)
    } else if (requestmode == 1 && processnode == 3) {
      List(1, 1, 1)
    } else {
      List(0, 0, 0)
    }
  }

  //参与竞价数、竞价成功数、DSP广告消费成本、DSP广告成本
  def dsp(iseffstive: Int, isbilling: Int,
          isbid: Int,
          iswin: Int,
          adorderid: Int,
          winPrice:Double,
          adPayment:Double): List[Double] = {
    var a = 0
    var b = 0
    var c = winPrice
    var d = adPayment
    if (isbilling == 1 && iseffstive == 1 && isbid == 1){
      a=1
    }
    if(isbilling == 1 && iseffstive == 1&&iswin==1&&adorderid!=0){
      b = 1
    }
    if(isbilling == 1 && iseffstive == 1&&iswin==1){
      c = c/1000.0
      d = d/1000.0
    }
    List(a,b,c,d)
  }


  //展示数、点击数
  def chick(requestmode:Int,iseffstive: Int): List[Double] ={
    if(requestmode==2&&iseffstive==1){
      List(1,0)
    }else if(requestmode==3&&iseffstive==1){
      List(0,1)
    }else{
      List(0,0)
    }
  }



















}
