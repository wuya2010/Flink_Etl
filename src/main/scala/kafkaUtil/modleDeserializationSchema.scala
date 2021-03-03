package kafkaUtil

import com.google.gson.Gson
import main.scala.HbseUtil.getHbseConnect.modle
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
 * @date 2021/2/23 11:49
 */
class modleDeserializationSchema extends  KafkaDeserializationSchema[modle]{

  override def isEndOfStream(nextElement: modle): Boolean = false

  //record： topic, message
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): modle = {
    val gson = new Gson()
    val getv = record.value()
//    println(getv.toString)  //fixme: 需要加一个过滤
    //此方法将指定的Json反序列化为指定类的对象  fixme： 转化为样例类
    gson.fromJson(new String(record.value(),"utf-8"),classOf[modle])
  }

  override def getProducedType: TypeInformation[modle] = {
    TypeInformation.of(new TypeHint[modle] {})
  }

}
