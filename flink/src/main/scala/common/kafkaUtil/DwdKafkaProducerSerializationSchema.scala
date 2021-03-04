package common.kafkaUtil

import java.nio.charset.Charset

import main.scala.kafkaUtil.getKafkaConnect.TopicAndValue
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/23 14:56
 */
//定义topic序列化 生产着
class DwdKafkaProducerSerializationSchema() extends KeyedSerializationSchema[TopicAndValue]{

    //给定序列化id

  //给空
  override def serializeKey(element: TopicAndValue): Array[Byte] = null

  override def serializeValue(element: TopicAndValue): Array[Byte] = {
    element.value.getBytes(Charset.forName("utf-8"))
  }

  //获取目标同批次
  override def getTargetTopic(element: TopicAndValue): String = {
      element.topic
  }
}
