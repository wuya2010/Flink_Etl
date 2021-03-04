package common.kafkaUtil

import main.scala.kafkaUtil.getKafkaConnect.TopicAndValue
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/23 14:32
 */

class TopicAndValueDeserializationSchema  extends KafkaDeserializationSchema[TopicAndValue] {
  //流是否有最后一条元素: 无界流，所以false
  override def isEndOfStream(t: TopicAndValue): Boolean = {
    false
  }

  //反序列化方法，生成一个 topicAndValue，来一条数据重新组成一条
  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TopicAndValue = {
    consumerRecord.value() //获取value ， topic
    //返回一个对象
    new TopicAndValue(consumerRecord.topic(), new String(consumerRecord.value(), "utf-8"))
  }

  //告诉flink 数据类型： 写法固定
  override def getProducedType: TypeInformation[TopicAndValue] = {
    TypeInformation.of(new TypeHint[TopicAndValue] {}) //写出topicAndValue
  }
}
