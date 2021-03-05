package etl.media

import com.alibaba.fastjson.JSONObject
import etl.media.MediaPostStreamEtl.media_post_result
import etl.media.utils.ConfigureUtil
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
    conf.set("hbase.zookeeper.quorum", ConfigureUtil.getProperty("hbase.zookeeper.quorum"))//"192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
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
    var streamResult:JSONObject = null
    //解析json 转样例类
    val jsonObj = com.alibaba.fastjson.JSON.parseObject(value)
    if( jsonObj.isInstanceOf[JSONObject]){
      //转换为样例类
      streamResult = com.alibaba.fastjson.JSON.parseObject(value)
    }

    val table = connection.getTable(TableName.valueOf(tableName))
    //怎么生成rowkey,将rowkey带过来
//    joinObject.put("parent_id",input.rowkey)
//    joinObject.put("type",input.`type`)
//    joinObject.put("post_user_name",input.post_user_name)
//    joinObject.put("post_user_name",input.post_content)
//    joinObject.put("post_content",input.dw_gofish_media_id)
//    joinObject.put("post_content",input.media)
//    joinObject.put("sub_industry_id",input.sub_industry_id.mkString(",")) //将set集合转换为string
//    joinObject.put("parent_id",parent_id.mkString(","))

    //    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("post_content"),Bytes.toBytes(streamResult.post_content))
    //    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("type"),Bytes.toBytes(streamObj.getString("type")))
    //    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("industry"),Bytes.toBytes(streamObj.getString("industry")))
    //    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("parent_id"),Bytes.toBytes(streamObj.getString("parent_id")))

    import scala.collection.JavaConverters._
    val fieldNameSet = jsonObj.keySet().asScala.diff(Set("rowkey"))
    val put = new Put(Bytes.toBytes(streamResult.getString("rowkey")))
    //数据写入
    fieldNameSet.map(field => {
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(field),Bytes.toBytes(streamResult.getString(field)))
    })

    //表的数据写入
    table.put(put)

    table.close()
  }
}
