package etl.media.utils

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/25 13:42
 */
trait FlinkEnv {

  private var flinkEnv:StreamExecutionEnvironment = null

  def getFlinkKafkaConnect: StreamExecutionEnvironment ={

    // fixme: FixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=3, backoffTimeMS=10000)

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
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))

    //监控指定的流，避免数据量过大

    //将env
    this.flinkEnv = env
    flinkEnv
  }


  //获取env的初始化
  def getInitFlink={
    synchronized{
      if(flinkEnv == null){
        getFlinkKafkaConnect
      }
      flinkEnv
    }
  }

}
