package common.kafkaUtil

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

import scala.collection.mutable

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/3/2 15:59
 */
object getKafkaConnect2 {
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
    //val stateBackend: StateBackend = new RocksDBStateBackend("hdfs://mycluster/flink/checkpoint")
    //env.setStateBackend(stateBackend)

    //设置重启策略  ： 重启3次 间隔10秒
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))


    // note:
//    kafka-0.10.1.X版本之前: auto.offset.reset 的值为smallest,和,largest.(offest保存在zk中)
//
//    kafka-0.10.1.X版本之后: auto.offset.reset 的值更改为:earliest,latest,和none
    //    (offest保存在kafka的一个特殊的topic名为:__consumer_offsets里面)


// note:
//    flink 消费 kafka 数据，提交消费组 offset 有三种类型
//
//    1、开启 checkpoint ：                                                  在 checkpoint 完成后提交
//    2、开启 checkpoint，禁用 checkpoint 提交：             不提交消费组 offset
//      3、不开启 checkpoint：                                              依赖kafka client 的自动提交


    //kafka : 设置参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","101.36.109.223:9092,101.36.109.7:9092,101.36.109.94:9092")  //连接的 kafka 节点
    //可以自定义消费者id
    properties.setProperty("group.id","wang")  //使用group的话，连接时带上groupid，topic的消息会分发到不同的consumer上，每条消息只被消费1次
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    //加入不设置
//    properties.setProperty("enable.auto.commit","false") //不自动提交
    properties.setProperty("auto.offset.reset","latest")  //从最近的偏移量开始获取数据

    /**
     * fixme: latest /earliest ：
     * 1. 设置了earliest, 被消费过的数据记录了offset
     * 2.
     *
     */



    //fixme: 方法2： 定义样例类,加载数据后成样例类, 这种方式不够直观
//    val kakfaStreammodle = env.addSource(new FlinkKafkaConsumer[modle]("testTopic", new modleDeserializationSchema, properties))

    val kakfaStreammodle = env.addSource(new FlinkKafkaConsumer[String]("testTopic", new SimpleStringSchema, properties))


    // fixme:
//    //自定flink 消费kafka的偏移量位置
//    val consumer = new FlinkKafkaConsumer[String]("testTopic", new SimpleStringSchema(), properties)
//
//    //设置偏移量
//    var offsets = mutable.Map[KafkaTopicPartition,java.lang.Long]()
//    offsets.put(new KafkaTopicPartition("testTopic", 0), 5l)
//    offsets.put(new KafkaTopicPartition("testTopic", 1), 5l)
//    offsets.put(new KafkaTopicPartition("testTopic", 2), 5l)
//
//
//    /**
//     * Flink从topic中最初的数据开始消费
//     */
//    consumer.setStartFromEarliest()
//
//    /**
//     * Flink从topic中指定的时间点开始消费，指定时间点之前的数据忽略
//     */
//    consumer.setStartFromTimestamp(5l)
//
//    /**
//     * Flink从topic中指定的offset开始，这个比较复杂，需要手动指定offset``
//     */
//    import scala.collection.JavaConverters._
//    consumer.setStartFromSpecificOffsets(offsets.asJava)  //fixme: FlinkKafkaConsumer010 没有setStartFromSpecificOffsets这个方法怎么办
//
//    /**
//     * Flink从topic中最新的数据开始消费
//     */
//    consumer.setStartFromLatest()
//
//    /**
//     * Flink从topic中指定的group上次消费的位置开始消费，所以必须配置group.id参数
//     */
//    consumer.setStartFromGroupOffsets()
//


    kakfaStreammodle.print()

    env.execute("get kafka data ..")

  }

}
