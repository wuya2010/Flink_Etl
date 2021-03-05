package etl.media

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/3/3 16:01
 */
object sparkKafkaProducer{

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("spark_gofish").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkconf)
      .enableHiveSupport() //是否直接写入外部hive
      .getOrCreate()




  }

}
