package main.scala.HbseUtil

import common.HbseUtil.{HbaseSink, singleHbaseSink}
import main.scala.EsUtil.getEsFlinkConncect.weichat
import org.apache.flink.streaming.api.scala._


/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/1/26 16:50
 */
object getHbseConnect {  //将测试数据，写入hbase

  case class modle(name:String,age:Int,time:String,timestamp:Long)
  case class test(name:String,age:Int,time:String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //将数据写入hbase
    val inputStream = env.readTextFile("E:\\Project\\Flink_datawork\\src\\main\\scala\\Common.EsUtil\\flinkDemo.txt")

    val dataStream = inputStream.map(x=>{
      val weiArr = x.split(",")
      test(weiArr(0).toString, weiArr(1).toInt,weiArr(2).toString)
    })

    //指向的是同一样例类, 正常写入
    dataStream.addSink(new singleHbaseSink)


    dataStream.print()

    env.execute("write to hbase")
  }
}
