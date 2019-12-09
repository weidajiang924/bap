package com.qf.dmp.app

import com.qf.dmp.bean.LogBean
import com.qf.dmp.util.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ToParquet {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[LogBean]))
    val spark: SparkSession = SparkUtil.getSession(conf)

    val ds: Dataset[String] = spark.read.textFile("C:\\Users\\34439\\Desktop\\学习\\spark项目\\DMP\\src\\data\\input\\2016-10-01_06_p1_invalid.1475274123982.log")
    import spark.implicits._
    ds.map(_.split(",",-1)).filter(_.length >= 85).map(arrs => {


      LogBean(arrs(0),
        toInt(arrs(1)),
        toInt(arrs(2)),
        toInt(arrs(3)),
        toInt(arrs(4)),
        arrs(5),
        arrs(6),
        toInt(arrs(7)),
        toInt(arrs(8)),
        arrs(9).toDouble,
        arrs(10).toDouble,
        arrs(11),
        arrs(12),
        arrs(13),
        arrs(14),
        arrs(15),
        arrs(16),
        toInt(arrs(17)),
        arrs(18),
        arrs(19),
        toInt(arrs(20)),
        toInt(arrs(21)),
        arrs(22),
        arrs(23),
        arrs(24),
        arrs(25),
        toInt(arrs(26)),
        arrs(27),
        toInt(arrs(28)),
        arrs(29),
        toInt(arrs(30)),
        toInt(arrs(31)),
        toInt(arrs(32)),
        arrs(33),
        toInt(arrs(34)),
        toInt(arrs(35)),
        toInt(arrs(36)),
        arrs(37),
        toInt(arrs(38)),
        toInt(arrs(39)),
        arrs(40).toDouble,
        arrs(41).toDouble,
        toInt(arrs(42)),
        arrs(43),
        arrs(44).toDouble,
        arrs(45).toDouble,
        arrs(46),
        arrs(47),
        arrs(48),
        arrs(49),
        arrs(50),
        arrs(51),
        arrs(52),
        arrs(53),
        arrs(54),
        arrs(55),
        arrs(56),
        toInt(arrs(57)),
        arrs(58).toDouble,
        toInt(arrs(59)),
        toInt(arrs(60)),
        arrs(61),
        arrs(62),
        arrs(63),
        arrs(64),
        arrs(65),
        arrs(66),
        arrs(67),
        arrs(68),
        arrs(69),
        arrs(70),
        arrs(71),
        arrs(72),
        toInt(arrs(73)),
        arrs(74).toDouble,
        arrs(75).toDouble,
        arrs(76).toDouble,
        arrs(77).toDouble,
        arrs(78).toDouble,
        arrs(79),
        arrs(80),
        arrs(81),
        arrs(82),
        arrs(83),
        toInt(arrs(84)))
    }
    ).write.parquet("C:\\Users\\34439\\Desktop\\学习\\spark项目\\DMP\\src\\data\\output\\log")

    spark.stop
  }

  def toInt(str: String): Int = {
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
  }
}
