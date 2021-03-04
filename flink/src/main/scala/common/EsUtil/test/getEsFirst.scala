package common.EsUtil.test

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import scala.collection.mutable

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/1 17:53
 */
object getEsFirst {

  case class weichat(name: String, age: Int, time: String)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))


    val input = env.readTextFile("E:\\WORKS\\Es_Hbase\\FlinkOnline\\src\\main\\scala\\flinkDemo.txt")


    val config = mutable.Map[String, String]();
    config.put("cluster.name", "my-cluster-name");
    //该配置表示批量写入ES时的记录条数
    config.put("bulk.flush.max.actions", "1");

    //fixme: 参数设置方式，对于private的构造器报错
    //1、用来表示是否开启重试机制//1、用来表示是否开启重试机制
    config.put("bulk.flush.backoff.enable", "true")
    //2、重试策略，又可以分为以下两种类型
    //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
    config.put("bulk.flush.backoff.type", "EXPONENTIAL")
    //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
    config.put("bulk.flush.backoff.type", "CONSTANT")
    //3、进行重试的时间间隔。对于指数型则表示起始的基数
    config.put("bulk.flush.backoff.delay", "2")
    //4、失败重试的次数
    config.put("bulk.flush.backoff.retries", "3")



    //    var transportAddresses: util.ArrayList[InetSocketAddress] = new util.ArrayList[InetSocketAddress]()
    //    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.18.151"), 19200))
    //    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.18.149"), 19200))


    //    // java 构建客户端
    //    val client = new RestHighLevelClient(RestClient.builder( //生成环境下ip
    //      new HttpHost("192.168.18.151", 19200, "http"),
    //      new HttpHost("192.168.18.149", 19200, "http"))
    //      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
    //        override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
    //          httpClientBuilder.disableAuthCaching
    //          val baseProvider = new BasicCredentialsProvider
    //          baseProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "H84I4fw6fDgdenuNRgfe"))
    //          val httpAsyncClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(baseProvider)
    //          httpAsyncClientBuilder
    //        }
    //      }))


    /**
     * bulkRequestsConfig: Map[String, String],
     * httpHosts: List[HttpHost],
     * elasticsearchSinkFunction: ElasticsearchSinkFunction[T],
     * failureHandler: ActionRequestFailureHandler,
     * restClientFactory: RestClientFactory)
     */

    val httpHosts: util.List[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.18.151", 19200, "http"))
    httpHosts.add(new HttpHost("192.168.18.149", 19200, "http"))


    //    //fixme: 无法加载该方法 , 缺少参数
    //    val t =   new ElasticsearchSink[String](config.asJava, httpHosts, new ElasticsearchSinkFunction[String](){
    //
    //            def createIndexRequest(element: String): IndexRequest = {
    //              // 用HashMap作为插入es的数据类型
    //              val sourceData = new util.HashMap[String,String]()
    //              sourceData.put("name", element)
    //    //          sourceData.put("age", element.age.toString)
    //    //          sourceData.put("time", element.time)
    //              Requests.indexRequest.index("weichat").`type`("_doc").source(sourceData)
    //            }
    //            override
    //            def process(weichat: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    //              requestIndexer.add(createIndexRequest(weichat))
    //            }
    //       })


    // fixme: 创建一个esSink的builder , 不需要设置密码的 es 连接
    val esSinkBuilder = new ElasticsearchSink.Builder[weichat](httpHosts,

      new ElasticsearchSinkFunction[weichat] {

        override def process(element: weichat, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          // 用HashMap作为插入es的数据类型， 用map的方式插入数据
          val sourceData = new util.HashMap[String, String]()
          sourceData.put("name", element.name)
          sourceData.put("age", element.age.toString)
          sourceData.put("time", element.time)

          // 创建一个index request， 相同类型
          val indexRequest = Requests.indexRequest().index("weichat").`type`("_doc").source(sourceData)
          // 用indexer发送请求
          indexer.add(indexRequest)
          println(element + " saved successfully") //数据写入成功
        }
      })

    }

}
