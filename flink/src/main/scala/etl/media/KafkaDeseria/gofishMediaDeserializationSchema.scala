package etl.media.KafkaDeseria

import com.google.gson.Gson
import etl.media.MediaPostStreamEtl.{media_post, media_post_full}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSONObject

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/25 14:12
 */
class gofishMediaDeserializationSchema extends KafkaDeserializationSchema[media_post_full]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  //是否是流的结束
  override def isEndOfStream(t: media_post_full): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): media_post_full = {
    val gson = new Gson()
//    //增加判断
//    val content = new String(consumerRecord.value(),"utf-8")
//    println(content)
//    val obj = com.alibaba.fastjson.JSON.parseObject(content)
//    var model:media_post_full = null
//    if(obj.isInstanceOf[JSONObject]){
//      model = gson.fromJson(new String(consumerRecord.value(), "utf-8"), classOf[media_post_full])
//    }
//    model

    try {
      gson.fromJson(new String(consumerRecord.value(), "utf-8"), classOf[media_post_full])
    } catch {
      case e:Exception => {
        logger.error("data formate is abnormal... ")
        println("data formate is abnormal...")
        null  //异常直接跳出
      }
    }

  }

  //这个的作用？
  override def getProducedType: TypeInformation[media_post_full] = {
    TypeInformation.of(new TypeHint[media_post_full] {})
  }
}
