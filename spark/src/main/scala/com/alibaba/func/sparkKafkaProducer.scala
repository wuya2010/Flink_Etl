package main.scala.com.alibaba.func

import java.util.Properties

import main.scala.com.alibaba.util.{ConfigureUtil, SparkBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql._

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/3/3 16:01
 */
object sparkKafkaProducer extends SparkBuilder{

  def main(args: Array[String]): Unit = {

    //读取表 dw_gofish_media_post
    val spark = getSparkSession
    val DM_GOFISH_POST = "dm:gofish_media_post"

    //从hbase读取数据生成rdd
    val post_rdd = getHbaseRdd(spark, DM_GOFISH_POST)

    import spark.implicits._

    //1.读取hbase的rdd,根据样例类字段生成df
    val post_content_df = post_rdd.map(x => (
      Bytes.toString(x._2.getRow),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("type"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("post_user_name"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("post_content"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("dw_gofish_media_id"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("media")))
    ))
      .toDF("rowkey", "type", "post_user_name", "post_content", "dw_gofish_media_id", "media") //获取kafka-time

    val result = post_content_df.toJSON //JSON strings

    println("get test...")

//    result.show(200, false) //直接可转换为json


        def getKafkaProperties = {
          //kafka : 设置参数
          val properties = new Properties()
          properties.setProperty("bootstrap.servers", ConfigureUtil.getProperty("bootstrap.servers"))//"101.36.109.223:9092,101.36.109.7:9092,101.36.109.94:9092")  //连接的 kafka 节点
          properties.setProperty("group.id","wang")  //使用group的话，连接时带上groupid，topic的消息会分发到10个consumer上，每条消息只被消费1次
          properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          properties.setProperty("auto.offset.reset","earliest")  //从最近的偏移量开始获取数据
          //返回
          properties
        }


        //将数据都发送到 kafka , 然后flink 流计算
        //向kafak中写入数据

        //
        result.rdd.foreachPartition(partition => {
          val producer = new KafkaProducer[String, String](getKafkaProperties)
          partition.map(message=>{
            val record = new ProducerRecord[String, String]("media_test", message)
            producer.send(record)
          })
          //在work端创建连接对象
          producer.close()

        } )



    }
  }

