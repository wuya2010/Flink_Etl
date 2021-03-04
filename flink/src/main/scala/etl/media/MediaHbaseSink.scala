package etl.media

import com.alibaba.fastjson.JSONObject
import etl.media.MediaPostStreamEtl.media_post_result
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
class MediaHbaseSink  extends RichSinkFunction[String]{ //参数 In 输入

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
  override def invoke(value: String, context: SinkFunction.Context): Unit = {
     write2Hbase("test_wang", value)
  }

  def write2Hbase(tableName: String, value: String) = {

    //获取样例类
    var streamResult:media_post_result = null
    //解析json 转样例类
    val jsonObj = com.alibaba.fastjson.JSON.parseObject(value)
    if( jsonObj.isInstanceOf[JSONObject]){
      streamResult = com.alibaba.fastjson.JSON.parseObject(value,classOf[media_post_result])
    }




    val table = connection.getTable(TableName.valueOf(tableName))
    //怎么生成rowkey,将rowkey带过来
    val put = new Put(Bytes.toBytes(System.currentTimeMillis()))
//    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("post_user_name"),Bytes.toBytes())
//    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("post_content"),Bytes.toBytes(streamResult.post_content))
//    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("type"),Bytes.toBytes(streamObj.getString("type")))
//    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("industry"),Bytes.toBytes(streamObj.getString("industry")))
//    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("parent_id"),Bytes.toBytes(streamObj.getString("parent_id")))

    //表的数据写入
    table.put(put)

    table.close()
  }
}
