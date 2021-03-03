package etl.media.KafkaDeseria

import com.google.gson.Gson
import etl.media.MediaPostStreamEtl.media_post
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
 * @date 2021/2/25 14:12
 */
class gofishMediaDeserializationSchema extends KafkaDeserializationSchema[media_post]{

  //是否是流的结束
  override def isEndOfStream(t: media_post): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): media_post = {
    val gson = new Gson()
    gson.fromJson(new String(consumerRecord.value(),"utf-8"),classOf[media_post])
  }

  //这个的作用？
  override def getProducedType: TypeInformation[media_post] = {
    TypeInformation.of(new TypeHint[media_post] {})
  }
}
