package Statistics

import org.apache.spark.sql.Row

object locationHandel {
  def getRequest(row:Row):List[Double]={
    var list = List[Double]()
    val requestmode = row.getAs[Int]("requestmode")
    val processnode = row.getAs[Int]("processnode")
    if(requestmode == 1){
      if(processnode ==3)
        List[Double](1,1,1)
      else if(processnode>=2){
        List[Double](1,1,0)
      } else
        List[Double](1,0,0)
    } else
      List[Double](0,0,0)
  }

  def getad(row:Row)={
    val iseffective = row.getAs[Int]("iseffective")
    val isbilling = row.getAs[Int]("isbilling")
    val isbid = row.getAs[Int]("isbid")
    val iswin = row.getAs[Int]("iswin")
    val adorderid = row.getAs[Int]("adorderid")
    val winprice = row.getAs[Double]("winprice")
    val adpayment = row.getAs[Double]("adpayment")
    if(iseffective == 1 && isbilling ==1 && isbid ==1){
      if(iseffective == 1 && isbilling ==1 && iswin ==1 && adorderid != 0)
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      else
        List[Double](1,0,0,0)
    }else
      List[Double](0,0,0,0)
  }

  def getShow(row:Row):List[Double]={
    var list = List[Double]()
    val requestmode = row.getAs[Int]("requestmode")
    val iseffective = row.getAs[Int]("iseffective")
    if(iseffective == 1){
      if(requestmode == 3)
        List[Double](0,1)
      else if(requestmode == 2){
        List[Double](1,0)
      } else
        List[Double](0,0)
    } else
      List[Double](0,0)
  }
}
