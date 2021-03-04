package common.HbseUtil

import main.scala.HbseUtil.getHbseConnect.{modle, test}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/3/2 10:39
 */
class singleHbaseSink  extends RichSinkFunction[test]{ //参数 In 输入

  var connection: Connection =_

  override def open(parameters: Configuration): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    connection = ConnectionFactory.createConnection(conf)
  }


  override def close(): Unit = {
      super.close()
      connection.close()
  }

  //触发写入
  override def invoke(value: test, context: SinkFunction.Context): Unit = {
     write2Hbase("test_wang", value)
  }

  def write2Hbase(tableName: String, value: test) = {

    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(System.currentTimeMillis()))
    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(value.name))
    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(value.age))
    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("time"),Bytes.toBytes(value.time))

    //表的数据写入
    table.put(put)

    table.close()
  }
}
