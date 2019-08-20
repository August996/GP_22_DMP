package com.ETL

import java.util.Properties

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
    val log_data: DataFrame = sqlContext.read.parquet(inputPath)
    val data: DataFrame = log_data.select("provincename","cityname")
    data.registerTempTable("t_log")
    val provinceAndCityAndCount: DataFrame = sqlContext.sql("select count(1) ct,provincename,cityname from t_log group by provincename,cityname")
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","root")
    val url = "jdbc:mysql://hadoop01:3306/test?useUnicode=true&characterEncoding=UTF-8"
    provinceAndCityAndCount.write.mode(SaveMode.Append).jdbc(url,"Count_provinceAndCity",prop)
    sc.stop()
  }
}
