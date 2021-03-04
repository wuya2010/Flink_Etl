package etl.media

import java.util

import com.alibaba.fastjson
import etl.media.KafkaDeseria.gofishMediaDeserializationSchema
import etl.media.utils.{FlinkEnv, FlinkUtils, HttpUtil}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.mutable
//fixme: 避免算子报错
import org.apache.flink.streaming.api.scala._


/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/25 8:37
 */
object MediaPostStreamEtl extends FlinkEnv {

  def main(args: Array[String]): Unit = {

    //获取环境
    val env = getInitFlink
    val properties = FlinkUtils.getKafkaProperties

        //从kafka读取数据
        val kafka_gofish_media: FlinkKafkaConsumer[media_post_full] =
          new FlinkKafkaConsumer[media_post_full]("media_test", new gofishMediaDeserializationSchema, properties)

        val inputStream = env.addSource(kafka_gofish_media)


    //将数据写入es
    //    val inputStream = env.readTextFile("E:\\Project\\Flink_datawork\\src\\main\\scala\\etl\\media\\media.txt")
    //      //定义时间
    //      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[media_post](Time.milliseconds(2500)) {
    //        override def extractTimestamp(element: media_post): Long = element.timeStamp
    //      } )


    //字段清洗
    val etl_media_stream = inputStream.map(media => {
      //字段位置
      media_post(media.rowkey,media.`type`,media.post_user_name,
        etl_word(media.post_content),media.dw_gofish_media_id,media.media)
    })
      .filter(!_.post_content.equals("")) //过滤得到非空的信息  //BoundedOutOfOrdernessTimestampExtractor


    //过滤行业不在范围内的数据
    def industryFilter(industry:mutable.Set[String]): Boolean ={
      var flag = false
      industry.map(x=>{
        if(x.matches("0|75|907|230|847|449|36|757|745")) //75,907,230,847,449,36,757,745
          flag = true
      })
      flag
    }

    //调用http请求，获取字段对应的vocation信息,生成3个字段
    //流数据 map
    val resultStream = etl_media_stream.map(media => {

      val content = media.post_content
      //传入参数, 返回多个结果，需要接卸
      val param = Seq(("data_text", content), ("proba","1"))
      val industryArr= getHttpVocation(content, param)
      media_post_industry(media.rowkey,media.`type`,media.post_user_name,media.post_content,
        media.dw_gofish_media_id,media.media,industryArr)  //类型industry:mutable.Set[String]
    })
      //过滤得到二级行业为：75,907,230,847,449,36,757,745
     .filter(message => industryFilter(message.sub_industry_id))


    //获取hbase静态表，流join,获取最终结果，rowkey
    //传入参数：input: DataStream[IN],asyncFunction: AsyncFunction[IN, OUT],timeout: Long,timeUnit: TimeUnit, capacity: Int)
    val result = AsyncDataStream.orderedWait(resultStream, new HbaseAsyncFunc2, 10l, java.util.concurrent.TimeUnit.SECONDS, 10)
    result.print()


    //返回json 串： {"post_user_name":"\"Committee on Ways and Means GOP\"","post_content":"SALE Board Free: (2pcs) chalk... pen,black pen etc","parent_id":"Set(626)","industry":"Set(667, 659)","type":"\"1\""}
//    result.addSink(new MediaHbaseSink)
//    result.addSink((new MedisEsSink).esBuilder.build())


    env.execute("gofish_media")


  }

  //如果少字段会报错？从Hbase将数据写入kafka,会将用到的数据写入kafaka
  //apply_type, appley_ids , status, creator, editor,
  case class media_post_full(
                              rowkey:String,
                              `type`:String,
                              post_user_name:String,
                              post_content:String,
                              dw_gofish_media_id:String,
                              media:String
                            )

  case class media_post(
                         rowkey:String,
                         `type`:String,
                         post_user_name:String,
                         post_content:String,
                         dw_gofish_media_id:String,
                         media:String
                       )

  case class media_post_industry(
                                  rowkey:String,
                                  `type`:String,
                                  post_user_name:String,
                                  post_content:String,
                                  dw_gofish_media_id:String,
                                  media:String,
                                  sub_industry_id: mutable.Set[String]
                                )

  case class media_post_result(
                                rowkey:String,
                                `type`:String,
                                post_user_name:String,
                                post_content:String,
                                dw_gofish_media_id:String,
                                media:String,
                                sub_industry_id: String,
                                parent_id:String
                              )


  //根据字段进行清洗
  def etl_word(message: String) = {
    /**
     * 清洗规则：
     * 1. 清洗 https://包含的内容
     * 2. #开头的英文单词去掉
     * 3. tel.+数字 或者 tel:+数字
     * 4. Email Address: 获取 Email:
     * 5. www.xxx.com / www.xxx.com.cn ：fixme: 采用www.字符串.com 或者 www.字符串.com.cn的方式匹配出来，将一整句过滤掉
     * 6. 只保留英文内容: 将帖子里面的中文、符号、特殊符号、表情、链接、内容全部清洗掉，只保留英文部分的内容。
     * 7. 清洗后长度(30,500)
     */

    val str = message.replaceAll("#\\S+|https:\\S+|Tel. [0-9\\-\\,]+|Tel.[0-9\\-\\,]+|Email Address: \\S+|Email Address:\\S+", "") //取消网址的判断：www.(\S+)+\.com(\.cn)?
    val str0 = str.replaceAll("www.(\\S+)+\\.com(\\.cn)?","need etl website") //将网址替换掉
//    str0.startsWith("\\",)  //去除首尾引号

    println("begin etl data ...")

    //清洗后合并
    val etl_str0 = str0.split("\\.").map(_.trim.concat(".")).map(str => {
      //将多余的逗号去掉
      if(str.contains("need etl website")) {
//        str.replaceAll(".*","")  //fixme: replaceAll
         ""
      }else{
        val strSplit = str.split(",").map(_.trim).filter(x => x != "")
        strSplit.mkString(",")
      }
    }).mkString("")

  //去除unioncode编码
    val etl_str1 = etl_str0.substring(0, etl_str0.length-1).replaceAll("\\\\u\\w+","")//去掉最后的 . + 非英文字符
    .replaceAll("\\ {2,}", " ")


    var result_etlStr = ""
    val str_length = etl_str1.length

    if (str_length > 500 || str_length < 30) {
      result_etlStr
    } else {
      result_etlStr = etl_str1
    }
    result_etlStr
  }


  //获取 3 个字段的返回信息
  def getHttpVocation(fieldInfo:String , param: Seq[(String, String)]) = {

    val url = "http://152.32.187.216/predict"

    var vocationInfo = ""

    if (fieldInfo == null || "".equals(fieldInfo)) {
      vocationInfo
      null
    } else {

      //如果执行失败，重试一次
      var flag = ""
      var jsonObject: fastjson.JSONObject = null

      while (!flag.equals("成功")) {

        // println( post_httpRequest("http://152.32.187.216/predict", Seq(("data_text","SALE\\u2757\\ufe0f \\ud83c\\udd7f\\ufe0f380White Board \\ud83c\\udd7f\\ufe0f380 Free: \\u2714\\ufe0feraser (2pcs) \\u2714\\ufe0f1box chalk... \\u2714\\ufe0fblue pen,black pen etc."),
        //      ("proba","1"))))
        val getIndustry = HttpUtil.post_httpRequest(url, param)
        //请求没有成功需要重复请求
        jsonObject = com.alibaba.fastjson.JSON.parseObject(getIndustry)

        flag = jsonObject.getString("msg")
      }

  //: "\"([^\"]*)\""意思就是 双引号 开头,然后中间需要获取的内容不允许是 双引号的所有内容,然后在双引号结尾。
//      val arr_trans = jsonObject.getJSONArray("data").get(0).toString.replaceAll("[{}\"]","").split(",")
//        .map(x=> x.split(":")).toSet

      import scala.collection.JavaConverters._
      val arr_trans = com.alibaba.fastjson.JSON.parseObject(jsonObject.getJSONArray("data").get(0).toString).keySet().asScala
      //获取arr数据

      arr_trans
     }

   }
}
