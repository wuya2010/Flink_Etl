package etl.media.utils

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URI
import java.util

import com.alibaba.fastjson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    //生产
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
   * 将df数据全量写入hbase中，适合数据量比较小 (<1g)
   *
   * @param df
   * @param table_name
   * @param Family
   * @param rowkey
   */
  def writeDf2Hbase(spark: SparkSession, df: DataFrame, table_name: String, Family: String, rowkey: String) = {

    if(!df.isEmpty){

      println("ready write to hbase ....")

      val HbaseConf = HBaseConfiguration.create()
      val quorum = "hbase.zookeeper.quorum"
      val clientPort = "hbase.zookeeper.property.clientPort"

      val sc = spark.sparkContext
      //测试
      //    sc.hadoopConfiguration.set(quorum, "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")

      //生产
      sc.hadoopConfiguration.set(quorum, "10.50.117.228:2181,10.50.213.85:2181,10.50.32.74:2181")
      sc.hadoopConfiguration.set(clientPort, "2181")
      sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, table_name)
      //在IDE中设置此项为true，避免出现"hbase-default.xml"版本不匹配的运行时异常
      sc.hadoopConfiguration.set("hbase.defaults.for.version.skip", "true")
      //增加写入配置 ： CDH6.1.0默认单region的处理能力限制的较弱，通过修改表配置增加并行数和处理列数即可
      sc.hadoopConfiguration.set("hbase.region.store.parallel.put.limit.min.column.count", "200") //线程处理的最小列数由默认100改为200
      sc.hadoopConfiguration.set("hbase.region.store.parallel.put.limit", "100") //单region处理的并发数由默认10改为100

      val job = new Job(sc.hadoopConfiguration)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

      //需要将schema 中的rowkey过滤掉
      val name_field = df.schema.fields.map(x => (x.name, x.dataType)).toSeq.diff(Seq((rowkey, StringType)))
      //rdd转换后写入hbase
      df.rdd.map(x => {
        //默认获取rowkey 为String类型
        val put = new Put(Bytes.toBytes(x.getAs[String](rowkey)))
        //转换为 string
        name_field.map(y => {
          y._2 match {
            //hbase 写入是否考虑字段类型
            case StringType => {
              if(x.getAs[String](y._1) != null ){  //&&  !"".equals(x.getAs[String](y._1)
                put.addColumn(Bytes.toBytes(Family), Bytes.toBytes(y._1), Bytes.toBytes(x.getAs[String](y._1)))
              }
            }
            case IntegerType => {
              if(x.getAs[Int](y._1) != null){
                put.addColumn(Bytes.toBytes(Family), Bytes.toBytes(y._1), Bytes.toBytes(x.getAs[Int](y._1)))
              }
            }
            case LongType =>{
              if(x.getAs[Long](y._1) != null){
                put.addColumn(Bytes.toBytes(Family), Bytes.toBytes(y._1), Bytes.toBytes(x.getAs[Long](y._1)))
              }
            }
            case _ => throw new Exception("Pleace check your fields type")
          }
        })
        (new ImmutableBytesWritable, put)
        // 两种写入方式： saveAsNewAPIHadoopDataset
        // 依赖为 mapred
      }).coalesce(64).saveAsNewAPIHadoopDataset(job.getConfiguration)
    }else{
      println("write to hbase skip , increment data is null ......")
    }
  }


  /**
   * 对于数据集比较大的表，采用批量插入数据的方法
   * @param tableName
   * @param df
   * @param Family
   * @param rowkey ： 指定rowkey 的字段
   * @param rdd ： 默认为空
   * @return
   */
  def writeBigDf2Hbase(tableName: String, df: DataFrame, Family: String, rowkey: String, rdd: RDD[(ImmutableBytesWritable, KeyValue)] = null) = {

    var conf: Configuration = null
    try {
      conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "10.50.117.228:2181,10.50.213.85:2181,10.50.32.74:2181")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
    } catch {
      case e: Exception => e.printStackTrace()
    }

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName) //需要写入的表
    conf.setInt("hbase.rpc.timeout", 600000)
    conf.setInt("hbase.client.operation.timeout", 600000)
    conf.setInt("hbase.client.scanner.timeout.period", 1200000)
    //输出表的位置
    conf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName)
    //设置最大hfile数
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3200)

    //相关配置
    val hdfsFile = "hdfs://10.50.117.228:8020/HbaseTemp/hfile"
    val path = new Path(hdfsFile)
    //如果存在则删除
    val fileSystem = FileSystem.get(URI.create(hdfsFile), new Configuration())
    if (fileSystem.exists(path)) {
      fileSystem.delete(new Path(hdfsFile), true)
    }

    //通过连接获取相关参数
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    val locator = connection.getRegionLocator(TableName.valueOf(tableName))
    val table = connection.getTable(TableName.valueOf(tableName))

    //表描述器
    val TableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    //如果表不存在，则创建表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      TableDescriptor.addFamily(new HColumnDescriptor(Family)) //默认新建表 family
      admin.createTable(TableDescriptor)
    }


    /**
     * 解析rdd, 设置重分区
     */

    //需要将schema 中的rowkey过滤掉
    //使用bulk批量写入，需要rowkey + column名 都有序，这里对column名进行排序
    val name_field = df.schema.fields.map(x => (x.name, x.dataType)).toSeq.diff(Seq((rowkey, StringType))).sortBy(x => x._1)

    val trans_rdd = df.rdd.sortBy(x => x.getAs[String](rowkey)).map(row => {
      //        var kvList : Seq[KeyValue] = mutable.List()
      var kvList: mutable.Seq[KeyValue] = mutable.ListBuffer()
      val getRowkey = row.getAs[String](rowkey)
      //遍历循环
      name_field.map(y => {
        y._2 match {
          //hbase 写入是否考虑字段类型
          case StringType => {
            if(row.getAs[String](y._1) != null &&  !"".equals(row.getAs[String](y._1))){
              kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey), Bytes.toBytes(Family), Bytes.toBytes(y._1), Bytes.toBytes(row.getAs[String](y._1)))
              kvList
            }
          }
          case IntegerType => {
            if(row.getAs[Int](y._1) != null){
            kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey), Bytes.toBytes(Family), Bytes.toBytes(y._1), Bytes.toBytes(row.getAs[Int](y._1)))
            kvList
          }}
          case LongType => {
            if(row.getAs[Long](y._1) != null){
            kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey), Bytes.toBytes(Family), Bytes.toBytes(y._1), Bytes.toBytes(row.getAs[Long](y._1)))
            kvList
          }}
          case _ => throw new Exception("Pleace check your fields type....")
        }
      })
      (new ImmutableBytesWritable(Bytes.toBytes(getRowkey)), kvList)
    }).flatMapValues(_.iterator)


    trans_rdd.saveAsNewAPIHadoopFile(hdfsFile,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)

    println("ready write to hbase ....")
    //最终结果
    val load = new LoadIncrementalHFiles(conf)
    load.doBulkLoad(new Path(hdfsFile), admin, table, locator)
  }


  /**
   * 向hbase中插入一条多列的数据
   * @param tableName
   * @param rowkey
   * @param family
   * @param infoSeq
   */
  def insertCell2Hbase(tableName:String, rowkey:String, family:String, infoSeq:Seq[Seq[String]]) = {

    val HbaseConf = HBaseConfiguration.create()
    val quorum = "hbase.zookeeper.quorum"
    val clientPort = "hbase.zookeeper.property.clientPort"

    //hbase连接设置
    HbaseConf.set(clientPort, ConfigureUtil.getProperty(clientPort))
    HbaseConf.set(quorum, ConfigureUtil.getProperty(quorum))

    //获取hbase连接
    val connection = ConnectionFactory.createConnection(HbaseConf)
    val table = connection.getTable(TableName.valueOf(tableName))
    //获取put
    val put = new Put(Bytes.toBytes(rowkey))
    //插入一个值
    infoSeq.map(x=>{
      val fieldName = x.apply(0)
      val fieldValue = x.apply(1)
      //不为空则写入
      if(fieldValue != null) {
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue))
      }
    })

    //数据写入
    table.put(put)
    table.close()
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
   *  对比hbase与es中的rowkey，获取将es中不存在的数据， 再利用公共方法将数据写入es
   * @param spark
   * @param df
   * @param hbaseTableName
   * @return : 返回增量的 df , 需要根据样例类 加载进入 es 中
   */
  def hbaseSynchrodata2Es(spark:SparkSession, df:DataFrame, hbaseTableName:String ): DataFrame ={
    val es_tableName = hbaseTableName.replace(":","_")
    val es_id = getEsMetadataDf(spark, es_tableName).select(col("metcasea_id"))

    println("es count =============" + es_id.count)

    val lackData: DataFrame = df.join(es_id,df("rowkey")===es_id("meta_id"),"left_anti") //只会返回左边没有关联到右表的字段数据
    lackData
  }


  /**
   * 获取增量数据
   * @param spark
   * @param hbaseFulldf
   * @param hbaseTableName
   * @return ： 返回 es 中查询的到的最大更新时间，和获取增量 df
   *         flag = true，重跑数据
   */
  def getIncrementDf(spark:SparkSession, hbaseFulldf:DataFrame, hbaseTableName:String, suffix:String ="",flag:Boolean=false)={

    val esTableName = hbaseTableName.replace(":","_")
    //从增量表获取数据
    val hbaseScoreTabel = esTableName + suffix
    var person_update_time = ""

    //如果需要重新跑，这里传入true
    if(flag){
      person_update_time ="1970-01-01 00:00:00"
      println(s"current_update_time  ${hbaseScoreTabel}=================" + person_update_time)

    }else{
      //否则，从hbase 读取标记位
      val esTableName = hbaseTableName.replace(":","_")
      //从增量表获取数据
      val hbaseScoreTabel = esTableName + suffix
      val updateMap: mutable.Map[String, String] = getRowHbase("hdm:incremental_update_records", hbaseScoreTabel, "info")
      person_update_time = updateMap.getOrElse("update_time", "1970-01-01 00:00:00")
      println(s"current_update_time  ${hbaseScoreTabel}=================" + person_update_time)

    }

    //从es读取标记位的时间  fixme：数据量比较大，不要进行广播
    val es_gofish_update = getEsUpdataDf(spark, esTableName)
      .select(col("meta_id"), format_date_col2(col("update_time")) as "es_update_time") //这里update_time 可能有yyyy-mm-dd 或者 long
      .filter(unix_timestamp(col("es_update_time")) > unix_timestamp(lit(person_update_time)))
      .repartition(200)

    //放入缓存
    es_gofish_update.persist(StorageLevel.MEMORY_AND_DISK)

    //增量数据, 只获取右表关联到的右表
    val increment_df = hbaseFulldf.join(es_gofish_update, es_gofish_update("meta_id") === hbaseFulldf("rowkey"), "inner")
      .drop("meta_id", "es_update_time")

    val max_update_time = es_gofish_update.select(max("es_update_time")).head.getString(0)

    println(s"max_update_time  ${hbaseScoreTabel}=================" + max_update_time)

    (max_update_time,increment_df)

  }


  /**
   * 获取时间区间内数据
   * @param spark
   * @param hbaseFulldf
   * @param hbaseTableName
   * @param suffix
   * @return
   */
  def getCurrentUpdateDf(spark:SparkSession, hbaseFulldf:DataFrame, hbaseTableName:String, suffix:String ="", last_timeStamp:String)={

    val esTableName = hbaseTableName.replace(":","_")
    //从增量表获取数据
    val hbaseScoreTabel = esTableName + suffix
    val updateMap: mutable.Map[String, String] = getRowHbase("hdm:incremental_update_records", hbaseScoreTabel, "info")
    val person_update_time = updateMap.getOrElse("update_time", "1970-01-01 00:00:00")

    println(s"current_update_time  ${hbaseScoreTabel}=================" + person_update_time)

    val es_gofish_update = getEsUpdataDf(spark, esTableName)
      .select(col("meta_id"), format_date_col2(col("update_time")) as "es_update_time") //这里update_time 可能有yyyy-mm-dd 或者 long
      .filter(unix_timestamp(col("es_update_time")) <= unix_timestamp(lit(person_update_time)) and unix_timestamp(col("es_update_time")) > unix_timestamp(lit(last_timeStamp)))
      .repartition(200)

    es_gofish_update.persist(StorageLevel.MEMORY_AND_DISK)

    //增量数据, 只获取右表关联到的右表
    val current_df = hbaseFulldf.join(es_gofish_update, es_gofish_update("meta_id") === hbaseFulldf("rowkey"), "inner")
      .drop("meta_id", "es_update_time")

    (person_update_time,current_df)
  }


  /**
   * 根据DF生成rdd, 抽取rdd 中的id 字段，根据 id 进行覆写；
   * 与指定id信息不同的，不会改变
   *
   * @param spark
   * @param df
   * @param tabelName
   * @param IdFieldName
   */
  def writeDs2EsById(spark: SparkSession, df: Dataset[_], tabelName: String, IdFieldName: String) = {

    /*    //设置连接ES的参数
        val sc = spark.sparkContext.getConf
        sc.set("es.net.http.auth.user","elastic")
        sc.set("es.net.http.auth.pass","mxtZPtg0VYz2dxa907Oj")
        sc.set("es.nodes.wan.only","true") //设置为 true 以后，会关闭节点的自动 discovery，只使用 es.nodes 声明的节点进行数据读写操做
        sc.set("es.batch.write.retry.count","10")
        sc.set("es.batch.write.retry.wait","15")
        sc.set("es.index.auto.create","true")
        sc.set("es.nodes","10.50.124.155:19200,10.51.78.177:19200,10.50.160.121:19200")*/

    if(!df.isEmpty){
      val Rdd = df.rdd //不能在这里赋值空值，会改变样例类字段类型

      println("ready write to es ....")
      //根据指定rdd的 id 写入数据，但不包含 id
      EsSpark.saveToEs(Rdd, s"/$tabelName", Map("es.mapping.id" -> IdFieldName, "es.mapping.exclude"->IdFieldName))
    }else{
      println("write to es skip , increment data is null ...")
    }




//    val Rdd2 = df.rdd.map(x=>x.)
//
//    //根据指定rdd中的
//    EsSpark.saveToEs(Rdd, s"/$tabelName", Map("es.mapping.id" -> IdFieldName))
//   val sc = spark.sparkContext
//    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
//    val muc = Map("iata" -> "MUC", "name" -> "Munich")
//    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")
//    val airportsRDD: RDD[(Int, Map[String, String])] = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
//    airportsRDD.saveToEsWithMeta("iteblog5/2015")

  }


  /**
   * 不考虑es的_id, 在es构建相应的mapping 后，将数据写入es， 全覆盖写入
   *
   * @param result
   * @param table_name
   * @param SaveMode
   */
  def writeDf2Es(result: DataFrame, table_name: String, SaveMode: SaveMode) = {

    val esConf_map = Map(
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "mxtZPtg0VYz2dxa907Oj",
      "es.nodes.wan.only" -> "true",
      "es.batch.write.retry.count" -> "10", //默认是重试3次,为负值的话为无限重试(慎用)
      "es.batch.write.retry.wait" -> "15", //默认重试等待时间是10s.可适当加大
      "es.index.auto.create" -> "true",
      "es.nodes" -> "10.50.124.155:19200,10.51.78.177:19200,10.50.160.121:19200"
    )

    result.write
      .format("org.elasticsearch.spark.sql")
      .options(esConf_map)
      .mode(SaveMode)
      .save(table_name)
  }



  /**
   *   java 方法，会报错一些其他问题， 目前只支持插入单一类型的map, 还需完善
   * @param df
   * @param indexName： 表名
   * @param idField： index 字段名
   */
  def writeDs2EsByUpdate(df:DataFrame,indexName:String,idField:String,fieldType:String) ={

    //获取所有除了id字段的 类名 + 字段类型
    val name_field = df.schema.fields.map(x => (x.name, x.dataType)).toSeq.diff(Seq((idField, StringType)))

      val list: Seq[mutable.Map[String, mutable.Map[String, AnyRef]]] = df.rdd.mapPartitions(partition => partition.map(row=>{
      //新建 scala 集合
      val submap: mutable.Map[String, AnyRef] = mutable.Map[String,AnyRef]()
      val map: mutable.Map[String, mutable.Map[String, AnyRef]] = mutable.Map[String, mutable.Map[String, AnyRef]]()

      //获取 id 值
      val esId = row.getAs[String](idField)

      name_field.map(y =>{
          val key = y._1
          val value = row.getAs[String](y._1)
          submap.put(key, value)
        })

      map.put(esId,submap)
      map
    })).collect().toList  //都放在了driver, 太吃资源

    bulkUpdate2(indexName,list)
  }

  /**
   * 定义一个Scala版本的公共方法
   * @param indexName
   * @param list
   */
  def bulkUpdate2(indexName: String, list: Seq[mutable.Map[String, mutable.Map[String, AnyRef]]]) = {

    val bulkRequest = new BulkRequest

    for (i <- 0 until list.size) {

      val map: mutable.Map[String, mutable.Map[String, AnyRef]] = list.apply(i)
      val key = map.keySet.head
      //这里需要传入java 的map
      import scala.collection.JavaConverters._
      val value: util.Map[String, AnyRef] = map.get(key).get.asJava
      val request = new UpdateRequest(indexName, "_doc").id(key)
      request.doc(value, XContentType.JSON)
      bulkRequest.add(request)
      //执行
      val client = new EsUtils().client
      val bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT)
      System.out.println(bulk.status.toString)
    }
  }



  /**
   * fixme: 通过流的方式，无法获取外置文件（集群为cluster模式，需要在每一个节点上传文件），需要完善
   *
   * @param spark
   * @param soc_df        ：传入df,需要翻译的词组需要去重
   * @param trans_colname ：传入df中需要翻译的词
   * @return
   */
  def get_transword_df(spark: SparkSession, soc_df: DataFrame, trans_colname: String): DataFrame = {
    val sc = spark.sparkContext

    //设置若干变量
    val trans_string = new StringBuffer()
    var trans_list = ListBuffer[String]()
    val trans_map = mutable.HashMap[String, String]()
    //map内部无法将翻译,rdd转list
    val rdd_list = soc_df.rdd.map(x => {
      x.getAs[String](s"$trans_colname")
    }).collect().toList


    //读取指定路径下的文件
    var industry_read: FileInputStream = null
    var industry_write: FileOutputStream = null
    try {
      //         industry_read = new FileInputStream("config/industry_translate") //

      industry_read = new FileInputStream(new File("config/industry_translate")) //E:\WORKS\bigdata-weyes\src\main\resources
      var len: Int = 0
      val buf = new Array[Byte](1024)
      //获取文件内容
      while (len != -1) {
        //读取配置信息
        val str = new String(buf, 0, buf.length)
        trans_string.append(str.trim)
        len = industry_read.read(buf, 0, buf.length)
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        loggerData.error("加载翻译文件异常")
      }
    }
    //关闭输入流
    industry_read.close()

    var source_list = List[String]()
    //增加判断
    if (!trans_string.toString.equals("")) {
      source_list = trans_string.toString.split("\n").toList
    }
    //3. 将翻译后的结果集转换成map
    if (source_list.isEmpty) {
      loggerData.info("翻译文件首次加载，为空情况正常")
    } else {
      for (elem <- source_list) {
        try {
          val splits = elem.trim.split(",") //读取翻译文件
          trans_map.put(splits(0), splits(1))
        } catch {
          case e: Exception => {
            e.printStackTrace()
            loggerData.error("读取翻译文件信息异常")
          }
        }
      }
    }

    //如果文件中没有字段进行追加，否则不写入
    //      industry_write = new FileOutputStream("config/industry_translate",true)
    industry_write = new FileOutputStream(new File("config/industry_translate"), true)

    // 方案2： 对list集合中的数据进行翻译,目的：生成一张唯一对应的翻译的表，然后根据翻译主键进行join,得到最终结果集
    for (i <- 0 until rdd_list.size) {

      //增加判断,如果集合中有key,则不查询，否者走baidu
      val src_word = rdd_list(i)
      val key_word = trans_map.keySet

      //1. 翻译文件中是否存在
      if (key_word.isEmpty) {
        loggerData.info("首次加载，翻译文件需要全部写入")
        //控制翻译查询时间，每次翻译需要1s
        Thread.sleep(1000)
        val result_word = get_trans_info(src_word, "en")
        //每一次都读取文件中的内容，进行匹配
        val result_info = src_word + "," + result_word

        industry_write.write(result_info.getBytes)
        industry_write.write("\n".getBytes)
        industry_write.flush()
        //翻译文件结果集,直接读文件获取rdd
        //        trans_list.append(src_word + "," + result_word)

      } else {
        if (key_word.contains(src_word)) {
          loggerData.info("翻译信息可以直接从文件获取")
          //          trans_list.append(src_word + "," + trans_map.get(src_word).get)
        } else {
          //如果集合中没有找到
          Thread.sleep(1000)
          val result_word = get_trans_info(src_word, "en")
          //每一次都读取文件中的内容，进行匹配
          val result_info = src_word + "," + result_word
          industry_write.write(result_info.getBytes())
          industry_write.write("\n".getBytes())
          industry_write.flush()
          //          trans_list.append(src_word + "," + result_word)
        }
      }
    }

    //关闭流
    industry_write.close()

    //不用文件写出: 读取文件需要分发到集群每一个节点
    //      val result = sc.textFile("src/main/resources/industry_translate").map(x=>{
    //         val split = x.split(",")
    //         trans_base(split(0),split(1))
    //      }).toDF()
    //
    //      result

    null

  }


  /**
   * 获取翻译信息写入hdfs,保存格式csv  输出字段类型： "srcword", "transword"
   * todo：对于 null 的处理很繁琐
   *
   * @param spark
   * @param soc_df        ：传入df,需要翻译的词组需要去重 todo:这个操作要有
   * @param trans_colname ：传入df中需要翻译的词
   * @return
   */
  def get_transword_full_df(spark: SparkSession, soc_df: DataFrame, trans_colname: String, tableName: String,transToLang:String) = {
    //初始化spark
    val sc = spark.sparkContext

    //翻译结果集
    val init_Map = mutable.HashMap[String, String]()
    //map内部无法将翻译,rdd转list,数据全部放到 driver 端
    val rdd_list = soc_df.rdd.map(x => {
      x.getAs[String](s"$trans_colname").trim
    }).collect().toList


    //从初始化数据中获取
    val trans_df: DataFrame = getDataByCsv(spark, tableName)

    //翻译文件转换为map
    val trans_map = trans_df.rdd.map(x => {
      val key = x.getAs[String]("srcword")
      val value = x.getAs[String]("transword")
      (key, value)
    }).collect().toMap

    var result:DataFrame = null
    //比对文件
    try {
      for (i <- 0 until rdd_list.size) {

          //增加判断,如果集合中有key,则不查询，否者走baidu
          val src_word = rdd_list(i)
          //首次加载
          val key_word = trans_map.keySet

          //如果没有包含,需要重新翻译
        if(src_word == null || "".equals(src_word)) {

           init_Map.put(src_word, "")

        }else{
          //内部判断
          if (key_word.contains(src_word)) {
            loggerData.info("翻译信息可以直接从文件获取")
            //                  init_Map.put(src_word,src_word)
          } else {
            //如果集合中没有找到
            Thread.sleep(1000)
            try{
            val result_word = get_trans_info(src_word, transToLang)
            //每一次都读取文件中的内容，进行匹配
            val result_info = src_word + "," + result_word
            init_Map.put(src_word, result_word)
            }
            catch {
              //将所有的 null 异常，都给到结果
              case eNull: NullPointerException => {
                println("java.lang.NullPointerException one times")
                loggerData.error(s"src_word : ${src_word} trans occur some noknown wrong ======================")
                init_Map.put(src_word, "")
              }
              case e: Exception => {
                println("other Exception one times")
                throw e
              }
            }
          }}}
      } catch {
        case e: Exception => {
          loggerData.info("加载异常，中断翻译过程...")
          throw e
        }
    } finally {
        //新增加的结果追加到 csv 文件
        val trans_add_str = init_Map.map { case (k, v) => {
          (k, v)
        }
        }.toSeq
        val trans_df2 = sc.parallelize(trans_add_str).toDF("srcword", "transword")
        writeDf2Csv(trans_df2, tableName, SaveMode.Append, 20) //追加写入

        //完整结果返回
        var result_Map = init_Map ++ trans_map
        val result3 = result_Map.map { case (k, v) => {
          (k, v)
        }
        }.toSeq
        //返回
        result = sc.parallelize(result3).toDF("srcword", "transword")

     }

    result

  }


  /**
   * 初始化获取翻译信息，写入hdfs,保存格式csv,完成数据写入 , 输出字段类型： "srcword", "transword"
   *
   * @param spark
   * @param soc_df        ：传入df,需要翻译的词组需要去重
   * @param trans_colname ：传入df中需要翻译的词
   * @param transToLang   ：转英文： "en" , 中文："zh"
   * @return
   */
  def get_transword_init(spark: SparkSession, soc_df: DataFrame, trans_colname: String, tableName: String,transToLang:String): DataFrame = {

    //初始化spark
    val sc = spark.sparkContext

    //翻译结果集
    val init_Map = mutable.HashMap[String, String]()
    //map内部无法将翻译,rdd转list
    val rdd_list = soc_df.rdd.map(x => {
      x.getAs[String](s"$trans_colname").trim
    }).collect().toList

    var result:DataFrame = null

    try {
      for (i <- 0 until rdd_list.size) {
        //将结果写入
        val src_word = rdd_list(i)
        loggerData.info("首次加载，翻译文件逐条翻译")
        //控制翻译查询时间，每次翻译需要1s
        Thread.sleep(1000)
        if(src_word==null || "".equals(src_word)){
          loggerData.info("load null data......")
          init_Map.put(src_word, "")
        }else{
          val result_word = get_trans_info(src_word, transToLang)//转英文： "en" , 中文："zh"
          //每一次都读取文件中的内容，进行匹配
          init_Map.put(src_word, result_word)
        }
      }
    } catch {
      case e: Exception => {
        loggerData.info("加载异常，中断翻译过程...")
        throw e
      }
    } finally {
      //结果集写出
      val result_arr = init_Map.map {
        case (k, v) => {
          (k, v)
        }
      }.toSeq

      result = sc.parallelize(result_arr).toDF("srcword", "transword")
      writeDf2Csv(result, tableName, SaveMode.Overwrite, 20)
      loggerData.info("翻译字段写入完成")

    }
    result
  }


  /**
   *  增量跑批方法2
   * @param spark
   * @param df
   * @param trans_cols
   * @param filterCondition
   * @param transToLang
   * @param tableName
   * @return
   */
  def get_transword_full_df2(spark: SparkSession, df: DataFrame, trans_cols: String, filterCondition:String, transToLang:String , tableName: String ,channel:String = "baidu") = {
    //初始化spark
    val sc = spark.sparkContext

    //翻译结果集
    val init_Map = mutable.HashMap[String, String]()

    val init_df = df.select(trans_cols)
      .filter(etl_commonRule(col(trans_cols)).isNotNull)
      .na.fill("")
      .filter(!col(trans_cols).rlike(filterCondition))
      .filter(trim(col(trans_cols)) =!= "")
      .groupBy(trans_cols).agg(count("*") as s"count_${trans_cols}")

    //核对需要翻译的数据条数
    println(s"need trans nums==============${trans_cols}" + init_df.count)
    //map内部无法将翻译,rdd转list,数据全部放到 driver 端
    val rdd_list = init_df.rdd.map(x => {
      x.getAs[String](s"$trans_cols").trim
    }).collect().toList


    //从初始化数据中获取
    val trans_df: DataFrame = getDataByOrc(spark, tableName,"dw_gofish")

    //翻译文件转换为map
    val trans_map = trans_df.rdd.map(x => {
      val key = x.getAs[String](trans_cols)
      val value = x.getAs[String](s"trans_${trans_cols}")
      (key, value)
    }).collect().toMap

    var result:DataFrame = null
    //比对文件
    try {
      for (i <- 0 until rdd_list.size) {

        //增加判断,如果集合中有key,则不查询，否者走baidu
        val src_word: String = rdd_list(i)
        val key_word: Set[String] = trans_map.keySet

        //如果没有包含,需要重新翻译
        if (src_word == null || "".equals(src_word)) {

          init_Map.put(src_word, "")

        } else {
          //内部判断
          if (key_word.contains(src_word)) {
            loggerData.info("transword can be loaded from files....")
            //                  init_Map.put(src_word,src_word)
          } else {
            //如果集合中没有找到
            Thread.sleep(1001)
            try {
              var result_word = ""
              if(channel.equals("baidu")) {
                result_word = get_trans_info(src_word, transToLang) //百度获取翻译信息
              }else{
                result_word= get_transLang_http(src_word,transToLang) //http 获取翻译信息
              }
              //每一次都读取文件中的内容，进行匹配
               init_Map.put(src_word, result_word)
            } catch {
              //将所有的 null 异常，都给到结果
              case eNull: NullPointerException => {
                println("java.lang.NullPointerException one times")
                loggerData.error(s"src_word : ${src_word} trans occur some noknown wrong ======================")
                init_Map.put(src_word, "")
              }
              case e: Exception => {
                println("other Exception one times")
                throw e
              }
            }
          }}}
    } catch {
      case e: Exception => {
        loggerData.info("加载异常，中断翻译过程...")
        throw e
      }
    } finally {
      //新增加的结果追加到 csv 文件
      val trans_add_str: Seq[(String, String)] = init_Map.map { case (k, v) => {
        (k, v)
      }}.toSeq

      val trans_df2 = sc.parallelize(trans_add_str).toDF(trans_cols, s"trans_${trans_cols}")
      //空数据集写入会报null异常
      writeDf2Orc(trans_df2,tableName,SaveMode.Append,"dw_gofish")

      //完整结果返回
      var result_Map = init_Map ++ trans_map
      val result3 = result_Map.map { case (k, v) => {
        (k, v)
      }
      }.toSeq
      //返回
      result = sc.parallelize(result3).toDF(trans_cols, s"trans_${trans_cols}")

    }
    //新增加的结果追加到 orc 文件
    result
  }

  /**
   * 两种翻译方式说明： baidu 翻译接口差不多 1s 一次， 而调用 http 接口会比较慢，同等翻译需求的情况下， 4.6w 百度耗时 22hrs, 48mins, 36sec ， http 请求 37hrs, 49mins, 13sec 也没有完成
   * @param spark
   * @param df
   * @param trans_cols ： 需要翻译的字段
   * @param filterCondition ： 对不是哪些结构的数据进行翻译
   * @param transToLang ： 转换的语言
   * @param tableName ： 输出的表名
   * @return
   */
  def get_transword_init2(spark: SparkSession, df: DataFrame, trans_cols: String, filterCondition:String, transToLang:String , tableName: String, channel:String = "baidu") = {

    //初始化spark
    val sc = spark.sparkContext

    //翻译结果集
    val init_Map = mutable.HashMap[String, String]()

    val trans_df = df.select(trans_cols)
      .filter(etl_commonRule(col(trans_cols)).isNotNull)
      .na.fill("")
      .filter(!col(trans_cols).rlike(filterCondition))
      .filter(trim(col(trans_cols)) =!= "")
      .groupBy(trans_cols).agg(count("*") as s"count_${trans_cols}")

    //核对需要翻译的数据条数
    println(s"need trans nums==============${trans_cols}" + trans_df.count)
    //map内部无法将翻译,rdd转list
    val rdd_list = trans_df.rdd.map(x => {
      x.getAs[String](s"$trans_cols").trim
    }).collect().toList

    var result:DataFrame = null

    try{
     for (i <- 0 until rdd_list.size) {
        //将结果写入
        val src_word = rdd_list(i)
        loggerData.info("首次加载，翻译文件逐条翻译")
        //控制翻译查询时间，每次翻译需要1s
        if(src_word == null || "".equals(src_word)){
          loggerData.info("load null data......")
          init_Map.put(src_word, "")
        }else{
          //如果集合中没有找到
          Thread.sleep(1001)
          try {
            var result_word = ""
            if(channel.equals("baidu")) {
              result_word = get_trans_info(src_word, transToLang)
            }else{
              result_word= get_transLang_http(src_word,transToLang)
            }
            //每一次都读取文件中的内容，进行匹配
            val result_info = src_word + "," + result_word
            init_Map.put(src_word, result_word)
          }catch{
            //将所有的 null 异常，都给到结果
            case eNull: NullPointerException => {
              println("java.lang.NullPointerException one times")
              loggerData.error(s"src_word : ${src_word} trans occur some noknown wrong ======================")
              init_Map.put(src_word, "")
            }
            case e: Exception => {
              println("other Exception one times")
              throw e
            }}}}
        } catch {
          case e: Exception => {
            loggerData.info("加载异常，中断翻译过程...")
            throw e
          }
        } finally {
        //结果集写出
        val result_arr = init_Map.map {
          case (k, v) => {
            (k, v)
          }
        }.toSeq

        result = sc.parallelize(result_arr).toDF(trans_cols, s"trans_${trans_cols}")
        //输出路径
        writeDf2Orc(result,tableName,SaveMode.Overwrite,"dw_gofish")
      }

      result
  }

  /**
   * 根据传入字段获取翻译文件信息
   * 上传集群每次会改变本地ip地址
   *
   * @param source
   * @return
   */
  def get_trans_info(source: String, transToLang: String) = {

    val APP_ID: String = "20201202000634847"
    val SECURITY_KEY: String = "gbG3eLCEJ_c5JD1_541T"
    val api = new TransApi(APP_ID, SECURITY_KEY)

    //每一天都是动态ip,如何实现呢
    val getTrans = api.getTransResult(source, "auto", transToLang)
    //输出结果： {"from":"zh","to":"en","trans_result":[{"src":"\u5b66\u751f","dst":"student"}]}
    //输出异常，可以拿到ip信息
    loggerData.info(s"翻译源信息：$getTrans")
    val jsonObject = ParseJsonData.getJsonData(getTrans)
    val arr_trans = jsonObject.getJSONArray("trans_result")
    var result_word = ""
    try {
      result_word = ParseJsonData.getJsonData(arr_trans.get(0).toString).getString("dst").toLowerCase //{"dst":"student","src":"学生"} 统一转换为小写
    } catch {
      case eNull: NullPointerException => {
        println("翻译超时，请求次数频率过高，超过每S一次")
        throw eNull
      }
    }
    result_word
  }



  /**
   * 调用公司自己的 翻译接口， 用公共方法
   *  语种设别方式 ， 对于只有符号和数字的数据源不进行翻译
   * @param sourceLang： 源语种  默认不写入
   * @param transLang：目标转换语种
   * @return
   */
  def get_transLang_http(field: String,transLang:String, sourceLang:String = "auto") = {
    // 测试环境 http://api-data.cweyes.cn/dict/translation
    // 生产环境 https://api-data.weyescloud.com/dict/translation
    val url = "https://api-data.weyescloud.com/dict/translation"

    //需要传入的参数
    val transSeq = Seq(("is",sourceLang), ("need",transLang), ("translation",field))

    var transWord =""

    if(field == null || "".equals(field)){
      transWord
    }else{
      try {
        //{"code":0,"msg":"ok","data":{"auto":"你好","en":"Hello there"},"req_time":1609399118.585797}

        //如果执行失败，重试一次
        var flag = "request limit error"
        var jsonObject: fastjson.JSONObject = null

        while(flag.equals("request limit error")){

          val getTrans = HttpUtil.post_httpRequest(url,transSeq)
          jsonObject = ParseJsonData.getJsonData(getTrans)
          //如果获取到值 mag 为ok
          loggerData.info(getTrans)
          flag= jsonObject.getString("msg")
        }

        val arr_trans = jsonObject.getString("data")
        transWord = ParseJsonData.getJsonData(arr_trans).getString(transLang).toLowerCase.trim //{"dst":"student","src":"学生"} 统一转换为小写

      } catch {
        case eNull: NullPointerException => {
          println("java.lang.NullPointerException one times")
          transWord
        }
        case e :Exception =>{
          e.getStackTrace
        }
      }
    }
    transWord
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
   * 数据写入csv
   *
   * @param df
   * @param tableName
   */
  def writeDf2Csv(df: DataFrame, tableName: String, saveMode: SaveMode, partitonNum: Int): Unit = {
    //写入csv
    df.repartition(partitonNum).write.mode(saveMode).format("csv")
      .option("header", "true") //第一行不作为数据内容--作为标题,带表头
      .option("encoding", "utf-8")
      .save(s"/config/warehourse/$tableName")

  }


  /**
   * 保存数据格式为orc
   *
   * @param df
   * @param tableName
   * @param saveMode
   */
  def writeDf2Orc(df: DataFrame, tableName: String, saveMode: SaveMode,path:String = null): Unit = {
    //写入csv
    if(!df.isEmpty){
      if(path == null){
        //存放临时数据
        df.write.mode(saveMode).format("orc").save(s"/config/warehourse/test_data/$tableName")
      }else{
        //需要用到的中间结果
        df.write.mode(saveMode).format("orc").save(s"/config/warehourse/$path/$tableName")
      }
    }else{
      println("get data is empty, skip this step ...")
    }


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

  /**
   * 获取自定义的csc格式测试文件数据
   *
   * @param spark
   * @param table_name
   * @return
   */
  def getTestDataByCsv(spark: SparkSession, table_name: String): DataFrame = {

    val suffix = ".csv"
    val csvData = spark.read.format("csv")
      //           .schema(table_schema)   //只是读取部分数据，因此schema 信息暂时不完善 table_schema:StructType
      .option("delimiter", ",") //指定分割符,墨粉分隔符
      .option("header", "true") //第一行不作为数据内容，作为标题
      .option("inferSchema", "true") //自动推断类型
      .option("ignoreLeadingWhiteSpace", true) //裁剪前面的空格,默认为true
      .option("ignoreTrailingWhiteSpace", true) //裁剪后面的空格,默认为true
      .option("encoding", "utf-8")
      .option("sep", ",")
      .load(s"src\\test\\resource\\$table_name" + suffix)

    csvData
  }
}
