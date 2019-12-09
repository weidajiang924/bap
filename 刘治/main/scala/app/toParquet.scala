package app

import bean.log
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import util.TypeUtil


object toParquet {
  def main(args: Array[String]): Unit = {
    val spark: SparkConf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.testing.memory","471859200")
      .set("spark.debug.maxToStringFields","85")
      val session: SparkSession = SparkSession.builder().config(spark).getOrCreate()

    import session.implicits._
    val ds: Dataset[String] = session.read.textFile("G:\\qianfeng\\DMPProject\\DMP\\src\\main\\resources\\2016-10-01_06_p1_invalid.1475274123982.log")
    val value: Dataset[Array[String]] = ds
      .map(x => {
        x.split(",",x.length)
      }).filter(_.length >= 85)
    value.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val logds: Dataset[log] = value.map(arrs => {
        new log(arrs(0),
          TypeUtil.toInt(arrs(1))  ,
          TypeUtil.toInt(arrs(2)),
          TypeUtil.toInt(arrs(3)),
          TypeUtil.toInt(arrs(4)),
          arrs(5),
          arrs(6),
          TypeUtil.toInt(arrs(7)),
          TypeUtil.toInt(arrs(8)),
          TypeUtil.toDouble(arrs(9)),
          TypeUtil.toDouble(arrs(10)),
          arrs(11),
          arrs(12),
          arrs(13),
          arrs(14),
          arrs(15),
          arrs(16),
          TypeUtil.toInt(arrs(17)),
          arrs(18),
          arrs(19),
          TypeUtil.toInt(arrs(20)),
          TypeUtil.toInt(arrs(21)),
          util.TypeUtil.toZero(arrs(22)) ,
          util.TypeUtil.toZero(arrs(23)) ,
//          arrs(23),
          arrs(24),
          arrs(25),
          TypeUtil.toInt(arrs(26)),
          arrs(27),
          TypeUtil.toInt(arrs(28)),
          arrs(29),
          TypeUtil.toInt(arrs(30)),
          TypeUtil.toInt(arrs(31)),
          TypeUtil.toInt(arrs(32)),
          arrs(33),
          TypeUtil.toInt(arrs(34)),
          TypeUtil.toInt(arrs(35)),
          TypeUtil.toInt(arrs(36)),
          arrs(37),
          TypeUtil.toInt(arrs(38)),
          TypeUtil.toInt(arrs(39)),
          TypeUtil.toDouble(arrs(40)),
          TypeUtil.toDouble(arrs(41)),
          TypeUtil.toInt(arrs(42)),
          arrs(43),
          TypeUtil.toDouble(arrs(44)),
          TypeUtil.toDouble(arrs(45)),
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
          TypeUtil.toInt(arrs(57)),
          TypeUtil.toDouble(arrs(58)),
          TypeUtil.toInt(arrs(59)),
          TypeUtil.toInt(arrs(60)),
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
          TypeUtil.toInt(arrs(73)),
          TypeUtil.toDouble(arrs(74)),
          TypeUtil.toDouble(arrs(75)),
          TypeUtil.toDouble(arrs(76)),
          TypeUtil.toDouble(arrs(77)),
          TypeUtil.toDouble(arrs(78)),
          arrs(79),
          arrs(80),
          arrs(81),
          arrs(82),
          arrs(83),
          TypeUtil.toInt(arrs(84)))

      })

    logds.write.parquet("G:\\qianfeng\\DMPProject\\DMP\\src\\main\\resources\\parquet")
    session.stop()
  }
}
