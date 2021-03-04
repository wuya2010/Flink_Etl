package etl.media.utils

import java.io.{FileInputStream, InputStreamReader}
import java.util.concurrent.TimeUnit

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicNameValuePair

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
 * <p>Description: 发送 http 请求</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/31 8:48
 */
object HttpUtil {

  //获取 http 客户端
  def HttpClient():CloseableHttpClient = {
    val httpClient = HttpClientBuilder.create()
      .setConnectionTimeToLive(10000,TimeUnit.MILLISECONDS) //连接时间5000ms
      .build()
    httpClient
  }

//
//  //http  get请求
//  def get_httpRequest(url:String,param:Seq[(String,String)]): String ={
//    val builder = new URIBuilder(url)
//    if(param.nonEmpty){
//      param.foreach(x=>{
//        builder.addParameter(x._1,x._2)
//      })
//    }
//
//    //获取http客户端
//    val client = HttpClient
//    //http get 请求
//    val httpResponse = client.execute(new HttpGet(builder.build()))
//    val entity = httpResponse.getEntity()
//
//    var content = ""
//    if(entity != null){
//      val inputStream = entity.getContent()
//
//      content = Source.fromInputStream(inputStream).getLines().toString()
//      inputStream.close()
//    }
//    client.close()
//    content
//  }
//
//
//  //http post 请求
//  def post_httpRequest(url:String,param:Seq[(String,String)]): String ={
//
//    val httpPost = new HttpPost(url)
//
//    // 定义了一个list,其数据类型为NameValuePair.
//    // List<NameValuePair> param = new ArrayList<NameValuePair> 用于java像url发送Post请求
//    val listParms = new ArrayBuffer[NameValuePair]()
//
//    param.foreach(x=>{
//        listParms += new BasicNameValuePair(x._1,x._2)
//      })
//
//
//    //获取post entity
//    import scala.collection.JavaConverters._
//    val entity :UrlEncodedFormEntity = new UrlEncodedFormEntity(listParms.toList.asJava,"utf-8")  //fixme: 这里要转 tolist
//     httpPost.setEntity(entity)
//
//    //获取客户端连接
//    val client = HttpClient()
//    val httpResonse = client.execute(httpPost)
//    val postEntity = httpResonse.getEntity()
//
//    var content = ""
//    if(postEntity != null){
//      val inputStream = postEntity.getContent()
//      content = Source.fromInputStream(inputStream).getLines().toString()
//      inputStream.close()
//    }
//    client.close()
//    content
//  }


  def get_httpRequest(addr:String,param: Seq[(String,String)]):String={
    val builder=new URIBuilder(addr)
    if(param.nonEmpty){
      param.foreach(r=>{
        builder.addParameter(r._1,r._2)
      })
    }
    val client=HttpClient()
    println(builder.build().toString)
    val httpResponse = client.execute(new HttpGet(builder.build()))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    client.close()
    content
  }


  def post_httpRequest(addr:String,param: Seq[(String,String)]):String={

    val req=new HttpPost(addr)
    val listParms=new ArrayBuffer[NameValuePair]()
    param.foreach(r=>{
      listParms += new BasicNameValuePair(r._1,r._2)
    })

//    val url = "http://baike.baidu.com/api/openapi/BaikeLemmaCardApi?scope="+
    //    URLEncoder.encode("103", "UTF-8")+"&format="+
    //    URLEncoder.encode("json", "UTF-8")+"&appid="+
    //    URLEncoder.encode("379020", "UTF-8")+"&bk_key="+
    //    URLEncoder.encode(kw, "UTF-8")+"&bk_length="+
    //    URLEncoder.encode("600", "UTF-8")

//    println(url)

    import scala.collection.JavaConverters._

    val entity=new UrlEncodedFormEntity(listParms.toList.asJava,"UTF-8")
    req.setEntity(entity)
    val client= HttpClient()

    val httpResponse = client.execute(req)
    val resEntity = httpResponse.getEntity()
    var content = ""

    if (resEntity != null) {
      val inputStream = new InputStreamReader(resEntity.getContent(),"UTF-8")
      //获取流数据
      var len = 0
      val buf = new Array[Char](1024)
      val strFile = new StringBuffer
      //加载流
      while ( len  != -1 ) {
        val str = new String(buf, 0, len)
        strFile.append(str)
        len = inputStream.read(buf, 0, buf.length)
      }

      content = strFile.toString
      inputStream.close
    }
    client.close()
    content
  }

  def main(args: Array[String]): Unit = {

    //中文 ： zh-cn ；  英文： en
    println( post_httpRequest("http://152.32.187.216/predict", Seq(("data_text","SALE\\u2757\\ufe0f \\ud83c\\udd7f\\ufe0f380White Board \\ud83c\\udd7f\\ufe0f380 Free: \\u2714\\ufe0feraser (2pcs) \\u2714\\ufe0f1box chalk... \\u2714\\ufe0fblue pen,black pen etc."),
      ("proba","1"))))

  }


}
