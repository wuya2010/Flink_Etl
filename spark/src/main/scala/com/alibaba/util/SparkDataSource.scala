package main.scala.com.alibaba.util

import com.alibaba.fastjson
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * <p>Description: 数据读取通用方法</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/4 10:27
 */
trait SparkDataSource {

  var loggerData: Logger = LoggerFactory.getLogger(this.getClass)


  /**
   * 通过配置获取hbase对应表的rdd
   *
   * @param spark
   * @param table_name
   * @return
   */
  def getHbaseRdd(spark: SparkSession, table_name: String): RDD[(ImmutableBytesWritable, Result)] = {

    val sc = spark.sparkContext

    val HbaseConf = HBaseConfiguration.create()
    val quorum = "hbase.zookeeper.quorum"
    val clientPort = "hbase.zookeeper.property.clientPort"

    //hbase连接设置
    HbaseConf.set(clientPort, ConfigureUtil.getProperty(clientPort))
    HbaseConf.set(TableInputFormat.INPUT_TABLE, table_name)
    HbaseConf.set(quorum, ConfigureUtil.getProperty(quorum))

    //本地公网
//    HbaseConf.set("hbase.zookeeper.quorum", "101.36.110.126:2181,101.36.110.14:2181,101.36.109.94:2181")
    //测试
    //HbaseConf.set(quorum, "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")

    //获取得到对应的rdd
    sc.newAPIHadoopRDD(
      HbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
  }



  /**
   *  获取一行的所有数据
   * @param tableName
   * @param rowkey
   * @param family
   * @return
   */
  def getRowHbase(tableName:String, rowkey:String, family:String )={

    val HbaseConf = HBaseConfiguration.create()
    val quorum = "hbase.zookeeper.quorum"
    val clientPort = "hbase.zookeeper.property.clientPort"

    //hbase连接设置
    HbaseConf.set(clientPort, ConfigureUtil.getProperty(clientPort))
    HbaseConf.set(quorum, ConfigureUtil.getProperty(quorum))
    //本地测试
//    HbaseConf.set("hbase.zookeeper.quorum", "101.36.110.126:2181,101.36.110.14:2181,101.36.109.94:2181")

    //获取hbase连接
    val connection = ConnectionFactory.createConnection(HbaseConf) //连接配置加进去
    //获取表
    val table = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowkey))

    //返回所有结果
    val result = table.get(get)
    val cells = result.rawCells()

    val cellMap: mutable.Map[String, String] = mutable.Map[String,String]().empty
    for (cell <- cells) {
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      //获取列名信息
      val column = Bytes.toString(CellUtil.cloneQualifier(cell))
      cellMap.put(column,value)
    }

    table.close()
    cellMap
  }


  /**
   * 通过配置获取es表数据，生成dataframe
   *
   * @param spark
   * @param table_name
   * @return
   */
  def getEsFieldDf(spark: SparkSession, table_name: String,field_name:String): DataFrame = {

    val esConf_map = Map(
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "mxtZPtg0VYz2dxa907Oj",
      "es.nodes.wan.only" -> "true",
      "es.batch.write.retry.count" -> "10", //默认是重试3次,为负值的话为无限重试(慎用)
      "es.batch.write.retry.wait" -> "15", //默认重试等待时间是10s.可适当加大
      "es.index.auto.create" -> "true",
      "es.nodes" -> "10.50.124.155:19200,10.51.78.177:19200,10.50.160.121:19200",
      //获取元数据
      "es.scroll.size"->"10000", //es每次请求返回的数据
      "es.scroll.keepalive"->"1m", //设置滚动有效时间 , 默认10min
      "es.input.max.docs.per.partition"->"1000000", //每个分区的大小
      "es.http.timeout"->"5m", //es超时时间5min
      "es.internal.spark.sql.pushdown"->"true", //Whether to translate (push-down) Spark SQL into Elasticsearch Query DSL
      "es.read.metadata"->"true",
      "es.mapping.date.rich"->"false", //不解析时间字段，默认为 ture, 在这里不设置会报错读取时间异常
      "es.read.field.include"->field_name //除元数据信息外包含的东西
    )

    //读指定表面的es 的元数据信息
    val es_df: Dataset[Row] = spark.read.format("org.elasticsearch.spark.sql") //org.elasticsearch.spark.sql
      .options(esConf_map)
      .load(table_name).select(
      //这两个字段不要
      expr("_metadata._index") as "meta_index",
      expr("_metadata._id") as "meta_id",
      expr("_metadata._score") as "meta_score",
      col(field_name)
    )

    es_df
  }



  /**
   * 仅获取 es 的元数据信息
   * @param spark
   * @param table_name
   * @return ： 返回字段  mata_id， meta_score f...
   */
  def getEsUpdataDf(spark: SparkSession, table_name: String): DataFrame = {

    val esConf_map = Map(
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "mxtZPtg0VYz2dxa907Oj",
      "es.nodes.wan.only" -> "true",
      "es.batch.write.retry.count" -> "10", //默认是重试3次,为负值的话为无限重试(慎用)
      "es.batch.write.retry.wait" -> "15", //默认重试等待时间是10s.可适当加大
      "es.index.auto.create" -> "true",
      "es.nodes" -> "10.50.124.155:19200,10.51.78.177:19200,10.50.160.121:19200",
      //获取元数据
      "es.scroll.size"->"10000", //es每次请求返回的数据
      "es.scroll.keepalive"->"1m", //设置滚动有效时间 , 默认10min
      "es.input.max.docs.per.partition"->"1000000", //每个分区的大小
      "es.http.timeout"->"5m", //es超时时间5min
      "es.internal.spark.sql.pushdown"->"true", //Whether to translate (push-down) Spark SQL into Elasticsearch Query DSL
      "es.read.metadata"->"true",
      "es.mapping.date.rich"->"false", //不解析时间字段，默认为 ture, 在这里不设置会报错读取时间异常
      "es.read.field.include"->"update_time" //除元数据信息外包含的东西
    )

    //读指定表面的es 的元数据信息
    val es_df: Dataset[Row] = spark.read.format("org.elasticsearch.spark.sql") //org.elasticsearch.spark.sql
      .options(esConf_map)
      .load(table_name).select(
      //这两个字段不要
      expr("_metadata._index") as "meta_index",
      //      expr("_metadata._type") as "meta_type",
      expr("_metadata._id") as "meta_id",
      expr("_metadata._score") as "meta_score",
      col("update_time")
    ).orderBy(col("update_time"))

//    println(es_df.select(max("update_time")).head.getString(0))

    es_df
  }


  /**
   * 仅获取 es 的元数据信息
   * @param spark
   * @param table_name
   * @return ： 返回字段  mata_id， meta_score f...
   */
  def getEsMetadataDf(spark: SparkSession, table_name: String): DataFrame = {

    val esConf_map = Map(
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "mxtZPtg0VYz2dxa907Oj",
      "es.nodes.wan.only" -> "true",
      "es.batch.write.retry.count" -> "10", //默认是重试3次,为负值的话为无限重试(慎用)
      "es.batch.write.retry.wait" -> "15", //默认重试等待时间是10s.可适当加大
      "es.index.auto.create" -> "true",
      "es.nodes" -> "10.50.124.155:19200,10.51.78.177:19200,10.50.160.121:19200",
      //获取元数据
      "es.scroll.size"->"10000", //es每次请求返回的数据
      "es.input.max.docs.per.partition"->"1000000", //每个分区的大小
      "es.http.timeout"->"5m", //es超时时间5min
      "es.internal.spark.sql.pushdown"->"true", //Whether to translate (push-down) Spark SQL into Elasticsearch Query DSL
      "es.read.metadata"->"true",
      "es.read.field.include"->"_metadata"
    )

    //读指定表面的es 的元数据信息
    spark.read.format("org.elasticsearch.spark.sql") //org.elasticsearch.spark.sql
      .options(esConf_map)
      .load(table_name).select(
      //这两个字段不要
      expr("_metadata._index") as "meta_index",
//      expr("_metadata._type") as "meta_type",
      expr("_metadata._id") as "meta_id",
      expr("_metadata._score") as "meta_score"
    )
  }



  /**
   * 读取csv文件获取测试数据，自定义csv,不校验数据格式
   *
   * @param spark
   * @param tableName
   * @return
   */
  def getDataByCsv(spark: SparkSession, tableName: String): DataFrame = {

    val csvData = spark.read.format("csv")
      .option("delimiter", ",") //指定分割符,墨粉分隔符
      .option("header", "true") //第一行不作为数据内容，作为标题
      .option("inferSchema", "true") //自动推断类型
      .option("ignoreLeadingWhiteSpace", true) //裁剪前面的空格,默认为true
      .option("ignoreTrailingWhiteSpace", true) //裁剪后面的空格,默认为true
      .option("encoding", "utf-8")
      .option("sep", ",")
      .load(s"/config/warehourse/$tableName")
    //本地测试
    //  .load (s"E:\\config\\warehourse\\${tableName}")

    csvData
  }

  /**
   * 从指定路径获取orc文件
   *
   * @param spark
   * @param table_name
   * @return
   */
  def getDataByOrc(spark: SparkSession, table_name: String, path:String =null) = {
    if(path != null){
      spark.read.format("orc").load(s"/config/warehourse/$path/$table_name")
    }else{
      spark.read.format("orc").load(s"/config/warehourse/test_data/$table_name")
    }
  }
}
