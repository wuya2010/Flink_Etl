package common.HbseUtil

import java.math.BigInteger
import java.security.MessageDigest

import main.scala.HbseUtil.getHbseConnect.modle
import main.scala.kafkaUtil.getKafkaConnect.{TopicAndValue}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes


/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/23 14:57
 */
class HbaseSink extends RichSinkFunction[TopicAndValue]{   //指定输出样例类， 监控所有的流，是否造成大的压力

  var connection:Connection = _

  //连接hbase
  override def open(parameters: Configuration) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    connection = ConnectionFactory.createConnection(conf)
  }

    override def close()={
      super.close()
      connection.close()
    }

  //判断不同得topic数据写入  cantext: context？
  override def invoke(value: TopicAndValue, context: SinkFunction.Context): Unit = {
    value.topic match{
      case "topic1" => invokeBaseData("hbase:topic1",value)
    }

  }

  //kafka ： message
  def invokeBaseData(tableName:String, message: TopicAndValue) ={
    //kafka没有rowkey,所以需要重新生成，自定义md5
    //解析message
    val topic: String =  message.topic
    val message_str: String = message.value
    //解析样例类
    val kafkaObj: modle = com.alibaba.fastjson.JSON.parseObject(message_str,classOf[modle])

    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(makeMd5(kafkaObj.name)))  //对字段进行，自定义md5
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("adid"), Bytes.toBytes(kafkaObj.name))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("adname"), Bytes.toBytes(kafkaObj.age))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dn"), Bytes.toBytes(kafkaObj.time))

    //关闭表
    table.close()
  }


  //自定义md5加密
  def makeMd5(mess:String)={
    try {
      if (mess == null) {
        null
      }
      val md5 = MessageDigest.getInstance("md5")
      md5.update(mess.getBytes()) //跟新数据为md5
      val md5Digest = md5.digest()
      val integer = new BigInteger(1, md5Digest)
      val md5_str = integer.toString(32)
      md5_str
    } catch {
      case e:Exception => e.getStackTrace
        null  //返回值
    }
  }

}
