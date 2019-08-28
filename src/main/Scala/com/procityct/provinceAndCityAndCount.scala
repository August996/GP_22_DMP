package com.procityct

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.ETL
  * @date:2019/8/20
  */
object provinceAndCityAndCount {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if (args.length != 2){
      println("目录参数不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //设置压缩方式 使用Snappy方式进行压缩
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val log_data: DataFrame = sqlContext.read.parquet(inputPath)
    val data: DataFrame = log_data.select("provincename","cityname")
    data.registerTempTable("t_log")
    val provinceAndCityAndCount: DataFrame = sqlContext.sql("select count(1) ct,provincename,cityname from t_log group by provincename,cityname")
    //加载配置文件 需要使用对应依赖
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    provinceAndCityAndCount.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)
//    prop.put("user","root")
//    prop.put("password","root")
//    val url = "jdbc:mysql://hadoop01:3306/test?useUnicode=true&characterEncoding=UTF-8"
//    provinceAndCityAndCount.write.mode(SaveMode.Append).jdbc(url,"Count_provinceAndCity",prop)
//    provinceAndCityAndCount.write.partitionBy("provincename", "cityname").json("hdfs://hadoop01:9000/gp22/out-20190821-1")

    sc.stop()
  }
}
