package main.scala.com.alibaba.func

import main.scala.com.alibaba.util.SparkBuilder
import org.apache.hadoop.hbase.util.Bytes

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
      .toDF("rowkey","type","post_user_name","post_content","dw_gofish_media_id","media")  //获取kafka-time

    val result = post_content_df.toJSON //JSON strings

    println("get test...")

    result.show(200,false) //直接可转换为json

    //将数据都发送到 kafka , 然后flink 流计算


  }

}
