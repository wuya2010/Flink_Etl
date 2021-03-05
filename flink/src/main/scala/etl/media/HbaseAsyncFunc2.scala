package etl.media

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.alibaba.fastjson.JSONObject
import com.google.common.cache.{Cache, CacheBuilder}
import etl.media.MediaPostStreamEtl.media_post_industry
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/3/2 10:16
 */
class HbaseAsyncFunc2  extends RichAsyncFunction[media_post_industry, String]{

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

  override def timeout(input: media_post_industry, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(Array("timeout:" + media_post_industry)) //使用一组结果元素完成 ResultFuture
  }


  //输入类型： media_post_industry ； 输出类型：  ResultFuture[String]
  override def asyncInvoke(input: media_post_industry, resultFuture: ResultFuture[String]): Unit = {
    //12线程的资源池
    executorService.submit(new Runnable {  //def submit(task: Runnable): Future[_]

      override def run(): Unit = {
        try {
          //每一条数据建一次连接？
          val conf = HBaseConfiguration.create()
          //测试配置，统一配置
          conf.set("hbase.zookeeper.quorum", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181") //"101.36.110.126:2181,101.36.110.14:2181,101.36.109.94:2181")// //切换为实际生产
          conf.set("hbase.zookeeper.property.clientPort","2181")
          //获取连接
          val connection = ConnectionFactory.createConnection(conf)
          //流数据与hbase进行join
          val getJoinObject = getHbaseJoinData2(input, connection, cache)
          //complete(Array(resultJsonObject.toJSONString))
          val t = Array(getJoinObject.toString)
          val unit = resultFuture.complete(Array(getJoinObject.toString))  //输出stirng类型
          logger.info(s"flink process data ${input} join opertition has finish...")

        }catch {
          case e:Exception => resultFuture.complete(Array("error:" + e.printStackTrace()))
        }
      }
    })
  }


  def getHbaseJoinData2(input: media_post_industry, conn: Connection, cache: Cache[String, String]) = {

    //获取json 信息,这里有3个二级行业
    val sub_industry_id = input.sub_industry_id

    //通过二级行业获取对应的parent_id
    var parent_id= mutable.Set[String]().empty

    //从cache获取数据
    sub_industry_id.map{elem => {
      //查看缓存是否有这个结果
      var value = ""
      value = cache.getIfPresent(elem)
      //如果找不到或为空
      if(value == null || "".equals(value)) {
        val table = conn.getTable(TableName.valueOf("dwd:base_industry"))  //从hbase表中获取数据
        val get = new Get(Bytes.toBytes(elem)).addColumn(Bytes.toBytes("info"),Bytes.toBytes("parent_id"))
        val result = table.get(get).rawCells()
        //需要判断是否关联上
        if(result != null){
          for(cell<-result){
            value = Bytes.toString(CellUtil.cloneValue(cell))
            cache.put(elem,value)
            parent_id += value
          }
        }

      }
    }}

    //没有关联的数据给空
    val joinObject = new JSONObject()
    //返回结果: (post_user_name:String,`type`:String ,post_content: String,industry_1:String,industry_2:String,industry_3:String)
    joinObject.put("rowkey",input.rowkey)
    joinObject.put("type",input.`type`)
    joinObject.put("post_user_name",input.post_user_name)
    joinObject.put("post_content",input.post_content)
    joinObject.put("dw_gofish_media_id",input.dw_gofish_media_id)
    joinObject.put("media",input.media)
    joinObject.put("sub_industry_id",input.sub_industry_id.mkString(",")) //将set集合转换为string
    joinObject.put("parent_id",parent_id.mkString(","))

    //返回
    joinObject
  }


}
