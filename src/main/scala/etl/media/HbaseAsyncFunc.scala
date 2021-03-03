package etl.media

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.alibaba.fastjson.JSONObject
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable


/**
 * <p>Description: 异步io进行聚合</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/3/1 15:52
 */
class HbaseAsyncFunc  extends RichAsyncFunction[String, String] {  //参数： [IN, OUT]

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var executorService : ExecutorService = _
  var cache: Cache[String,String] = _  //key-value的形式

  override def open(parameters: Configuration): Unit = {
    //初始化线程池 12个线程
    executorService = Executors.newFixedThreadPool(12)
    //初始化缓存，减小hbase查询压力
    cache = CacheBuilder.newBuilder()
      .concurrencyLevel(12) //设置并发级别 允许12个线程同时访问
      .expireAfterAccess(2, TimeUnit.HOURS) //设置缓存 2小时 过期
      .maximumSize(10000) //设置缓存大小
      .build()
  }

  override def close(): Unit = {
    //优雅关闭
    ExecutorUtils.gracefulShutdown(100, TimeUnit.MILLISECONDS, executorService);
  }

  override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(Array("timeout:" + input)) //使用一组结果元素完成 ResultFuture
  }


  //处理每条数据
  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    //12线程的资源池
    executorService.submit(new Runnable {  //def submit(task: Runnable): Future[_]
      override def run(): Unit = {
        try {
          //每一条数据建一次连接？
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum","192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
          conf.set("hbase.zookeeper.property.clientPort","2181")
          //获取连接
          val connection = ConnectionFactory.createConnection(conf)
          //流数据与hbase进行join
          val getJoinObject = getHbaseJoinData(input, connection, cache)
          //complete(Array(resultJsonObject.toJSONString))
          val t = Array(getJoinObject.toString)
          val unit = resultFuture.complete(Array(getJoinObject.toString))
          logger.info(s"flink process data ${input} join opertition has finish...")

        }catch {
          case e:Exception =>
        }
      }
    })
  }

  //关联 hbase 获取一级行业 jvm详解
  def getHbaseJoinData(inputStream:String,conn:Connection,cache: Cache[String,String]) :JSONObject={

    //解析json,定义关键词，从cache获取数据
    val streamObject = com.alibaba.fastjson.JSON.parseObject(inputStream) //json str的格式
    //获取json 信息,这里有3个二级行业
    val industry_1 = streamObject.getString("industry_1")
    val industry_2 = streamObject.getString("industry_2")
    val industry_3 = streamObject.getString("industry_3")

    val industry = Array(industry_1,industry_2,industry_3)
    //通过二级行业获取对应的parent_id
    var parent_id = mutable.Set[String]()

   //从cache获取数据
   industry.map{elem => {
     //查看缓存是否有这个结果
     var value = ""
     value = cache.getIfPresent(elem)
     //如果找不到或为空
     if(value == null || "".equals(value)) {
       val table = conn.getTable(TableName.valueOf("dwd:base_industry"))  //从hbase表中获取数据
       val get = new Get(Bytes.toBytes(elem)).addColumn(Bytes.toBytes("info"),Bytes.toBytes("parent_id"))
       val result = table.get(get).rawCells()
       for(cell<-result){
         value = Bytes.toString(CellUtil.cloneValue(cell))
       }
       cache.put(elem,value)
       //将元素结果写入set
       parent_id += value
       table.close()
     }
    }}

    val joinObject = new JSONObject()
    //返回结果
    joinObject.put("parent_id",parent_id.toString())

    joinObject
   }


}
