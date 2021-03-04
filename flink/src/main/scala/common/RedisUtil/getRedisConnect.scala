package common.RedisUtil

import main.scala.HbseUtil.getHbseConnect.modle
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/23 10:22
 */
object getRedisConnect {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置账号密码的连接
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")   //输入文件

    val dataStream = inputStream
      .map(data => {

      })


    //数据写入redis, 方法丰富
//    dataStream.addSink(new RedisSink[modle]( config, new MyRedisMapper() ))

    dataStream.print()

    env.execute("redis sink modle")


  }
}


//自定义redis的格式
class  MyRedisMapper extends RedisMapper[modle]{

  //三个复写方法
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"get_modle")
  }

  override def getKeyFromData(t: modle): String = {
//    modle.id.toString
    ""
  }

  override def getValueFromData(t: modle): String ={
//    modle.id
    ""
  }
}
