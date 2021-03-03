package kafkaUtil

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.eclipse.jetty.util.thread.ExecutionStrategy.Producer

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/23 11:32
 */
object createKafkaProducer {

  //构建kafka 生产者发送数据
  def main(args: Array[String]): Unit = {

    //参数配置
    val props = new Properties
    props.put("bootstrap.servers", "101.36.109.223:9092,101.36.109.7:9092,101.36.109.94:9092")
    props.put("acks", "-1")
    props.put("batch.size", "16384")
    props.put("linger.ms", "10")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //put, setProperty 区别
//    properties.put()

    val producer = new KafkaProducer[String, String](props)
    val str = "test_message" //转string
    val record = new ProducerRecord[String, String]("testTopic", str)
    producer.send(record)

    producer.close()

  }
}
