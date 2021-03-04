package main.scala.com.alibaba.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * <p>Description: 初始化sparksession,并根据配置文件设置spark conf</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/4 10:27
 */
trait SparkBuilder extends SparkDataSource   {

  private var sparkSession :SparkSession = null

  def initSparkSession() = {
    //增加配置参数
    val sparkconf = new SparkConf().setAppName("spark_gofish").setMaster("local[*]")
    sparkconf.set("spark.sql.warehouse.dir","target/spark-warehouse")
    sparkconf.set("spark.kryoserializer.buffer.max", "1024mb")
    sparkconf.set("spark.driver.maxResultSize", "3gb")
    sparkconf.set("spark.sql.broadcastTimeout", "12000") //增加广播变量的处理时间
//    sparkconf.set("spark.sql.autoBroadcastJoinThreshold", "-1") //不自动广播小表
    //增加匹配
    sparkconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    //es 相关参数
    sparkconf.set("es.net.http.auth.user","elastic")
    sparkconf.set("es.net.http.auth.pass","mxtZPtg0VYz2dxa907Oj")
    sparkconf.set("es.nodes.wan.only","true") //设置为 true 以后，会关闭节点的自动 discovery，只使用 es.nodes 声明的节点进行数据读写操做
    sparkconf.set("es.batch.write.retry.count","10") //给定批次的重试次数，默认3
    sparkconf.set("es.batch.write.retry.wait","15") //由批量拒绝引起的批写入重试之间等待的时间，默认10s
    sparkconf.set("es.batch.size.entries","5000") //批写入的bulk大小，默认值是1000
    sparkconf.set("es.batch.write.refresh","false") //批量写入后再刷新: 分批写入的方式，容易中间丢数据而不知道， 对于大表这种方式不错
    sparkconf.set("es.nodes","10.50.124.155:19200,10.51.78.177:19200,10.50.160.121:19200")

    //写入测试路径
//    sparkconf.set("es.nodes","192.168.18.151:19200,192.168.18.149:19200")
//    sparkconf.set("es.net.http.auth.pass","H84I4fw6fDgdenuNRgfe")

//    sparkconf.set("es.nodes","101.36.110.65:19200,101.36.110.144:19200,101.36.110.117:19200") //本地
    //fixme: upsert 局部更新字段，需要调用这个参数
    sparkconf.set("es.write.operation","upsert")  //配置参数， 只更新指定字段
    sparkconf.set("es.update.retry.on.conflict","5")  //发生冲突时，重试次数



    //通过读取文件的方式读不到内容
//    val configLoader = ConfigFactory.parseFile(new File("config/application.conf"))

/*
    //1210通过配置方式获取不到资源
    val application_file = SparkBuilder.getClass.getResource("/application.conf").getPath
    val configLoader = ConfigFactory.parseFile(new File(application_file))

    //scala 集合与 java 集合互转
    import scala.collection.JavaConverters._
    val config = configLoader.getConfig("sparkConf").entrySet().asScala

    // No configuration setting found for key 'SparkConf'
    for(set <- config){
      println(set.getKey + "" + set.getValue.unwrapped().toString)
      sparkconf.set(set.getKey,set.getValue.unwrapped().toString)
    }*/

    val sparkSession = SparkSession.builder().config(sparkconf)
      .enableHiveSupport() //是否直接写入外部hive
      .getOrCreate()

    //返回值
    this.sparkSession = sparkSession //全部变量赋值

  }

  def getSparkSession = {
    synchronized{
      if(sparkSession == null){
        initSparkSession
      }
    }
    sparkSession
  }

//  def main(args: Array[String]): Unit = {
//    initSparkSession
//  }

}
