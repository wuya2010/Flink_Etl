package main.scala.kafkaUtil

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import common.HbseUtil.HbaseSink
import common.kafkaUtil.{DwdKafkaProducerSerializationSchema, TopicAndValueDeserializationSchema, modleDeserializationSchema}
import main.scala.HbseUtil.getHbseConnect.modle
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, FlinkKafkaProducer011}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/23 9:24
 */
object getKafkaConnect {

  case class TopicAndValue(topic:String, value:String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.getConfig.setAutoWatermarkInterval(300L)

    env.enableCheckpointing(60000L) //1分钟做一次checkpoint
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//仅仅一次
    env.getCheckpointConfig.setCheckpointTimeout(90000L)//设置checkpoint超时时间
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000L)//设置checkpoint间隔时间30秒
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置时间方式
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//设置时间模式为事件时间

    // 参数的设置
    //    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    //checkpoint设置
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //cancel时保留checkpoint
    //设置statebackend 为rockdb
    //    val stateBackend: StateBackend = new RocksDBStateBackend("hdfs://mycluster/flink/checkpoint")
    //    env.setStateBackend(stateBackend)

    //设置重启策略  ： 重启3次 间隔10秒
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))


    //kafka : 设置参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","101.36.109.223:9092,101.36.109.7:9092,101.36.109.94:9092")  //连接的 kafka 节点
//    properties.setProperty("group.id","modleTopic")  //使用group的话，连接时带上groupid，topic的消息会分发到不同的consumer上，每条消息只被消费1次
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","smallest")  //从最近的偏移量开始获取数据

//    smallest : 自动把offset设为最小的offset；
//    largest : 自动把offset设为最大的offset；


    //添加kafka source fixme: 2种方式
    //参数：topic: String, valueDeserializer: DeserializationSchema[T], props: Properties
    //将message内容字符串获取
    val kakfaStream = env.addSource(new FlinkKafkaConsumer[String]("", new SimpleStringSchema, properties))
    //对kafka获取数据进行解析,将json串装换位样例类, 获取时间戳信息
    //{"created_date": "2016-08-30t15", "expir_date": " 2021-08-30t15", "obj_name": "enom, llc", "tel": "", "domain": "s3-design.com", "media": "www.internic.net", "id": "c18e8cc3ba2ef35d9fbb34c286dcb2cf", "rank_long": 16139565959801369}
    val getKafkaStream = kakfaStream.map(str => {
      // 获取json 对象的内容
      //      val jsonObject = com.alibaba.fastjson.JSON.parseObject(str)
//      val create_date = jsonObject.getString("")

      //将结果放入样例类, 将 str 转换  ==> 可以在这里对字段进行清洗
      val modle1 = com.alibaba.fastjson.JSON.parseObject(str, classOf[modle])
      modle1
    })
//      .assignAscendingTimestamps()//增量时间戳
      .assignTimestampsAndWatermarks(   //生成watermarks
        //fixme:需要添加样例类，说明类型
          new BoundedOutOfOrdernessTimestampExtractor[modle](Time.milliseconds(2500)){  //允许迟到数据
            override def extractTimestamp(t: modle): Long = t.timestamp * 1000L
          }
    )


    //fixme: 方法2： 定义样例类,加载数据后成样例类, 这种方式不够直观

    //fixme: Flink 的 Kafka 消费者称为 FlinkKafkaConsumer08 (或Kafka 0.9.0.x的值为FlinkKafkaConsumer09)等等，或者只是Kafka >= 1.0.0版本的 FlinkKafkaConsumer )

    val kakfaStreammodle = env.addSource(new FlinkKafkaConsumer[modle]("testTopic", new modleDeserializationSchema, properties))
      //name:String,age:Int,time:String,timestamp:Long


    kakfaStreammodle.print()


    //可以指定topic: 同时监听多个topic
    import scala.collection.JavaConverters._
    val topicList = ListBuffer[String]("topic1","topic2","topic").asJava
    //返回topic的kafkaConsumer
    val kafkaTopic = new FlinkKafkaConsumer[TopicAndValue](topicList, new TopicAndValueDeserializationSchema, properties)
    // 设置消费模式（checkepoint没有设置时，才走这个）
    kafkaTopic.setStartFromEarliest()  // Specifies the consumer to start reading from the earliest offset for all partitions.

    //对kafka流进行过滤非 json 格式数据
    val allTopicStream = env.addSource(kafkaTopic).filter(topic => {
      val obj = com.alibaba.fastjson.JSON.parseObject(topic.value)
      obj.isInstanceOf[JSONObject]
    })

    //定义测流
    val sideOutHbaseTag = new OutputTag[TopicAndValue]("hbaseStream")

    //根据不同的  topic 写入不同的存储空间
    //process: creating a transformed output stream.
    val resultStream = allTopicStream.process(new ProcessFunction[TopicAndValue, TopicAndValue] {  //<I, O>
      //定义每个流的处理逻辑
      override def processElement(value: TopicAndValue, ctx: ProcessFunction[TopicAndValue, TopicAndValue]#Context, out: Collector[TopicAndValue]): Unit = {
        value.topic match {
          case "dd" | "aa" | "cc" => ctx.output(sideOutHbaseTag, value) //value 写入测流
          case _ => out.collect(value) //主流
        }
      }
    })

    //测流写入hbase
     resultStream.getSideOutput(sideOutHbaseTag).addSink(new HbaseSink)
    //主流写入kafka topic , 定义 kafkaProcuer
    resultStream.addSink(
      new FlinkKafkaProducer[TopicAndValue]("101.36.109.223:9092,101.36.109.7:9092,101.36.109.94:9092","",new DwdKafkaProducerSerializationSchema))//自定义：序列化类



//   聚合类操作
//    getKafkaStream.keyBy("id")
//      //划动窗口  (size: Time, slide: Time, offset: Time)  ， 窗口大小
//      .window(SlidingEventTimeWindows.of(Time.minutes(1),Time.seconds(10),Time.hours(-8)))
//      .allowedLateness(Time.milliseconds(1))  //如果不指定允许迟到，默认为 0 ； 只对时间时间窗口有效
//
//
//   //对窗口进行聚合 (size: Time, slide: Time)
//   getKafkaStream.keyBy("id")
//      .timeWindow(Time.hours(1),Time.minutes(5)) //划动窗口
//      .aggregate()


    //定义测输出流
    val outputTag = new OutputTag[modle]("late-data")  //侧输出流

    //数据发送到kafka
//    String topicId,
//    KeyedSerializationSchema<IN> serializationSchema,
//    Properties producerConfig,
//    Semantic semantic)

//    getKafkaStream.addSink(new FlinkKafkaProducer011[String]
//    ("mymodleTopic", new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),
//      properties, Semantic.EXACTLY_ONCE))


    //输出到kakfa

    //输出string
//    new FlinkKafkaProducer011[String]("modleTopic",
//      new KeyedSerializationSchemaWrapper(new SimpleStringSchema()), properties, Semantic.EXACTLY_ONCE)


    env.execute("consumer kafka")

  }


}
