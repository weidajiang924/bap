package test

import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.util.{Failure, Success, Try}

object HBaseTest {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    //设置Zookeeper的地址和端口来访问HBase，先从配置中读取，如配置中不存在，设置地址为localhost，端口为默认端口2181

    conf.set("hbase.zookeeper.quorum", "MyDis:2181")

    //创建操作HBase的入口connection
    val conn: Connection = ConnectionFactory.createConnection(conf)
    //创建操作HBase表的入口Admin
    val admin: Admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf("stu"))
    Try {
      //准备一个row key
      val p = new Put("rowKey".getBytes)
      //为put操作指定 column qualifier 和 value
      p.addColumn("info".getBytes, "qualifier".getBytes, "value".getBytes)
      //放数据到表中
      table.put(p)
      table.close()
    } match {
      case Success(_) => println("Done!")
      case Failure(e) => e.printStackTrace()
    }
  }
}
