package util

object StringUtil {
  def isNotBlank(str:String):Boolean={
    if (str == null || str.isEmpty)
      return false
    return true
  }
}
