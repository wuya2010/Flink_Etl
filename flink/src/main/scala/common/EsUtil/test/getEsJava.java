package common.EsUtil.test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.RestClientBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/4 10:59
 */
public class getEsJava {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  //设置并行度
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));


        DataStream<String> input = env.readTextFile("E:\\WORKS\\Es_Hbase\\FlinkOnline\\src\\main\\scala\\flinkDemo.txt");


        Map<String,String> config = new HashMap<String, String>();
        config.put("cluster.name", "my-cluster-name");
        //该配置表示批量写入ES时的记录条数

        config.put("bulk.flush.max.actions", "1");

        //增加配置
        //1、用来表示是否开启重试机制//1、用来表示是否开启重试机制

        config.put("bulk.flush.backoff.enable", "true");
        //2、重试策略，又可以分为以下两种类型
        //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
        config.put("bulk.flush.backoff.type", "EXPONENTIAL");
        //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
        config.put("bulk.flush.backoff.type", "CONSTANT");
        //3、进行重试的时间间隔。对于指数型则表示起始的基数
        config.put("bulk.flush.backoff.delay", "2");
        //4、失败重试的次数
        config.put("bulk.flush.backoff.retries", "3");
        config.put("es.net.http.auth.user", "elastic");
        config.put("s.net.http.auth.pass", "H84I4fw6fDgdenuNRgfe");


        /**
         * bulkRequestsConfig: Map[String, String],
         * httpHosts: List[HttpHost],
         * elasticsearchSinkFunction: ElasticsearchSinkFunction[T],
         * failureHandler: ActionRequestFailureHandler,
         * restClientFactory: RestClientFactory)
         */

        List<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("192.168.18.151", 19200, "http"));
        httpHosts.add(new HttpHost("192.168.18.149", 19200, "http"));



        RestClientFactory restClientFactory = new RestClientFactory() {
            public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {

            }
        };



        ActionRequestFailureHandler actionRequestFailureHandler = new ActionRequestFailureHandler() {
            public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {

            }
        };


//
//        //参数配置 , 这里5个参数
//        ElasticsearchSink<String> esSink = new ElasticsearchSink(config, httpHosts, new ElasticsearchSinkFunction<String>() {
//
//
//            public IndexRequest createIndexRequest(String element) {
//                Map<String, String> json = new HashMap<String,String>();
//                //将需要写入ES的字段依次添加到Map当中
//                json.put("data", element);
//
//                return Requests.indexRequest()
//                        .index("my-index")
//                        .opType("my-type")
//                        .source(json);
//            }
//
//            @Override
//            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                indexer.add(createIndexRequest(element));
//            }
//        }, new ActionRequestFailureHandler() {
//            @Override
//            public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {
//
//            }
//        }, new RestClientFactory() {
//
//            @Override
//            public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
////                RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
//
//                //客户端
//                 restClientBuilder = RestClient.builder(
//                        //生成环境下ip
//                        new HttpHost("192.168.18.151", 19200, "http"),
//                        new HttpHost("192.168.18.149", 19200, "http"))
//                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                                httpClientBuilder.disableAuthCaching();
//                                BasicCredentialsProvider baseProvider = new BasicCredentialsProvider();
//                                baseProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "H84I4fw6fDgdenuNRgfe"));
//                                HttpAsyncClientBuilder httpAsyncClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(baseProvider);
//                                return httpAsyncClientBuilder;
//                            }
//                        });
//
//            }
//        });

    }


}
