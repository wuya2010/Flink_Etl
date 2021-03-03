package etl.media

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.elasticsearch.common.xcontent.XContentType


/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/1/26 16:19
 */
class MedisEsSink  extends RichSinkFunction[String]{
  
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
    httpHosts.add(new HttpHost("192.168.18.151", 19200, "http"))
    httpHosts.add(new HttpHost("192.168.18.149", 19200, "http"))
  
  
   //用 bulider 建立连接 , 数据完成写入
   val esBuilder = new ElasticsearchSink.Builder(httpHosts, new ElasticsearchSinkFunction[String](){
            def createIndexRequest(element: String): IndexRequest = {

              val streamObj = com.alibaba.fastjson.JSON.parseObject(element)

              // 用HashMap作为插入es的数据类型
//              val sourceData = new util.HashMap[String, String]()
//              sourceData.put("name", element.name)
//              sourceData.put("age", element.age.toString)
//              sourceData.put("time", element.time)
              Requests.indexRequest.index("String").`type`("_doc").source(streamObj,XContentType.JSON) //解析json串
            }

            override
            def process(String: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
              requestIndexer.add(createIndexRequest(String))
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
