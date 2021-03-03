package etl.media.KafkaDeseria

import org.apache.hadoop.hbase.util.Bytes

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/3/3 16:01
 */
object sparkKafkaProducer {

  def main(args: Array[String]): Unit = {

    //读取表 dw_gofish_media_post
    //需求1
    val DM_GOFISH_POST = "dm:gofish_media_post"

    //增加邮箱合法性验证
    val DW_EMAIL_VALID = "dw:gofish_email_valid"
    val DW_GOFISH_COMPANY = "dw:gofish_company"

    //从hbase读取数据生成rdd
    val company_rdd = getHbaseRdd(spark, DM_GOFISH_COMPANY)
    val person_rdd = getHbaseRdd(spark, DM_GOFISH_PERSON)
    //获取邮箱合法性hbase表数据
    val email_rdd = getHbaseRdd(spark, DW_EMAIL_VALID)
    //获取手机号
    val dw_company_rdd = getHbaseRdd(spark, DW_GOFISH_COMPANY)

    import spark.implicits._

    //1.读取hbase的rdd,根据样例类字段生成df
    val company_df = company_rdd.map(x => (
      gofishCompany(
        Bytes.toString(x._2.getRow),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("id"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("address"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("address_new"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("apply_date"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("apply_location"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("apply_man"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("apply_num"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("apply_pub_num"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("apply_type"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("brand"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("brand_location"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("brand_name"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("brand_num"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("brand_register_num"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("brand_time"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("brand_type"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("brief"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("business_model"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("business_type"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("c_url"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("c_url2"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("c_url_new"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("c_url_status_new"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("city"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("company_dynamic_time"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("company_dynamic_title"))),
        //      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("company_dynamic_title"))),  //重复字段
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("company_mainMarket"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("company_num"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("company_state"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("country"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("country_code"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("country_code_iso"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("country_code_iso2"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("country_region"))),
        //      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("create_time"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("custom_status"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("district"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("dm_gofish_industry_id"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("domain_status"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("email"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("facebook"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("fax"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("financing"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("financing_investor"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("financing_value"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("img_src_keyword"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("img_url_keyword"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("importExport"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("instagram"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("investments_abroad_amount"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("investments_abroad_investor"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("investments_abroad_time"))), //加载报错
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("is_delete"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("kw"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("legal_proceedings_accused"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("legal_proceedings_accuser"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("legal_proceedings_event"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("legal_proceedings_time"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("legend_person"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("legend_person_title"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("linkedin"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("linkedin_status"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("logo"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("logo_src_keyword"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("logo_url_keyword"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("main_products"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("main_status"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("markers"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("marketing_annual_marketing_appraisement"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("marketing_annual_marketing_turnover"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("marketing_estimated_value"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("marketing_retained_profits"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("marketing_the_tax_credit_rating"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("media_status"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("new_address"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("obj_name"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("patent_expiration_date"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("patent_name"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("person_status"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("postcode_keyword"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("process_version"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("province"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("published_date"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("qualification_certificate_expire_time"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("qualification_certificate_img"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("qualification_certificate_name"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("qualification_certificate_start_time"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("recruitment_information_job_type"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("recruitment_information_number_of_people"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("recruitment_information_post_desc"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("recruitment_information_post_name"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("recruitment_information_update_time"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("recruitment_information_work_place"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("register_id"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("registered_capital"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("share_aggregate_market_value"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("share_eps"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("share_pe"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("sort_long"))), //转long
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("status"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("street"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("tel"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("tel_new"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("the_enterprise_referred_to_as"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("total_employees"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("total_employees_new"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("total_revenue"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("twitter"))),
        //      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("update_time"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("vocation"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("year_established"))),
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("year_established_new"))), //需要转换为date格式
        Bytes.toString(x._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("youtube"))))
      ))
      .toDF().repartition(200).as("dm_gofish_company")






  }

}
