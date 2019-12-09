package test

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

object tagToHBase {
  def tagToHbase()={
    val config = HBaseConfiguration.create
    config.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(TableName.valueOf("rec:user_rec"))

    // 举个例子而已，真实的代码根据records来
    val list = new java.util.ArrayList[Put]
    for(i <- 0 until 10){
      val put = new Put(Bytes.toBytes(i.toString))
      put.addColumn(Bytes.toBytes("t"), Bytes.toBytes("aaaa"), Bytes.toBytes("1111"))
      list.add(put)
    }
    // 批量提交
    table.put(list)
    // 分区数据写入HBase后关闭连接
    table.close()
  }
}
