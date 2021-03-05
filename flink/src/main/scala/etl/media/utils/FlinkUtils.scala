package etl.media.utils

import java.util
import java.util.Properties

import main.scala.EsUtil.getEsFlinkConncect.weichat
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/25 13:43
 */
object FlinkUtils{


  def getKafkaProperties = {
    //kafka : 设置参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.18.151:9092,192.168.18.152:9092,192.168.18.150:9092")//"101.36.109.223:9092,101.36.109.7:9092,101.36.109.94:9092")  //连接的 kafka 节点
    properties.setProperty("group.id","wang2")  //使用group的话，连接时带上groupid，topic的消息会分发到10个consumer上，每条消息只被消费1次
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","earliest")  //从最近的偏移量开始获取数据
    //返回
    properties
  }

  /**
   * 构建es的连接
   */
   def buildElasticsearchSink()={
     //设置es参数
     val config = new util.HashMap[String,String]()
     //该配置表示批量写入ES时的记录条数
     config.put("enable.auto.commit", "true")
     config.put("auto.commit.interval.ms", "1000")
     config.put("auto.offset.reset", "earliest")
     config.put("session.timeout.ms", "30000")
     config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

     //设置密码
     config.put("es.net.http.auth.user", "elastic")
     config.put("s.net.http.auth.pass", "H84I4fw6fDgdenuNRgfe")

     // 定义es的httpHost 配置信息
     val httpHosts = new util.ArrayList[HttpHost]()
     //测试连接
     httpHosts.add(new HttpHost("192.168.18.151", 19200, "http"))
     httpHosts.add(new HttpHost("192.168.18.149", 19200, "http"))

     //这里传入一个样例类， 这个方法通用化
     val esBuilder = new ElasticsearchSink.Builder(httpHosts, new ElasticsearchSinkFunction[weichat](){
       def createIndexRequest(element: weichat): IndexRequest = {
         // 用HashMap作为插入es的数据类型
         val sourceData = new util.HashMap[String, String]()
         // 连接新的方法
         sourceData.put("name", element.name)
         sourceData.put("age", element.age.toString)
         sourceData.put("time", element.time)
         Requests.indexRequest.index("weichat").`type`("_doc").source(sourceData)
       }

       override
       def process(weichat: weichat, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
         requestIndexer.add(createIndexRequest(weichat))
       }})


     //设置参数, 设置连接密码
     esBuilder.setRestClientFactory(new RestClientFactory(){
       override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
         restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback {
           override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
             val credentialsProvider = new BasicCredentialsProvider
             credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "H84I4fw6fDgdenuNRgfe"))
             httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
           }
         })
       }
     })

   }


}
