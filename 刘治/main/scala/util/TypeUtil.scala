package util

object TypeUtil {
  def toInt(str:String): Int ={
   try{
     str.toInt
   }catch{
     case _ :Exception => 0
   }

  }

  def toDouble(str:String): Double ={
    try{
      str.toDouble
    }catch{
      case _ :Exception => 0
    }
  }

  def toZero(str:String):String={
    var s = "0"
    if(str!=""){
      s = str
    }
    s
  }
}
