package etl.media.KafkaDeseria

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/2/25 15:16
 */
object testEtl {

  def main(args: Array[String]): Unit = {

//    val message = "#BosleyForReacher Thank You  www.baidu.com.cn. Tel. 813-445-4818, Tel. 727-495-7474, Email Address: yourname@yourcompany.com , Tel. 941-312-7815  The hunt for Reacher has spanned nearly 10 years.  To EVERY single person who has been with me during that chase: thank you from ...the bottom of my heart.  Let's crush Hollywood!!!  Your support will forever fuel me. https://vimeo.com/jeffbosley/      bosleyforreacherthankyou"
    val message = "my"


    //https://\S+ | #\S+ | tel.\d+ | tel:\d+ | Email Address:\+ | www.\S+ .com | www.\S+.com.cn


    val str = message.replaceAll("#\\S+|https:\\S+|Tel. [0-9\\-\\,]+|Tel.[0-9\\-\\,]+|Email Address: \\S+|Email Address:\\S+|www.(\\S+)+\\.com\\.cn?", "") //
    val str2: String =  str.replaceAll("\\ {2,}", " ")  // 2个或多个空格去掉

    println(str2)

    //清洗后合并
    val etl_str1 = str2.split("\\.{1}").map(_.trim.concat(".")).map(str=>{
      val strSplit = str.split(",").map(_.trim).filter(x=>x!="")
      strSplit.mkString(",")
    }).mkString("")

    var result_etlStr = ""
    val str_length = etl_str1.length

    if(str_length >500 || str_length <30){
      result_etlStr
    }else{
      result_etlStr = etl_str1
    }
    result_etlStr


    println(result_etlStr)


  }

}
