package com.Rpt

import com.util.{AppDictUtil, RptUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Rpt
  * @date:2019/8/21
  */
object Appt {
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

    //广播app_dict
    sc.setCheckpointDir("src/app_dict")
    val arr: RDD[Array[String]] = sc.textFile("src/data/app_dict.txt").map(line => {
      line.split("\t")
    }).filter(line => line.length != 0)
    val app_dict: RDD[(String, String)] = arr.map(x => {
      val appName = x(1)
      val appIp = x(4)
      (appIp -> appName)
    }).cache()
    app_dict.checkpoint()
    val appMap: Map[String, String] = app_dict.collect().toMap
    //统计各个指标
    val df: DataFrame = sqlContext.read.parquet(inputPath)
    val req = df.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      //key 值 appid  appname
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
      //创建三个对应方法处理指标
      val list1: List[Double] = RptUtils.request(requestmode, processnode)
      val list2: List[Double] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      if (appname.equals("其他")){
        (appMap.getOrElse(appid,"其他"), list1 ::: list2 ::: list3)
      }else{
        (appname, list1 ::: list2 ::: list3)
      }
    })
    val req1 = req.reduceByKey((x,y)=>(x.zip(y)).map(x=>x._1+x._2))
    val AppRow: RDD[Row] = req1.map(row => {
      val value = row._2
      Row(row._1,value(0), value(1), value(2), value(3), value(4), value(5), value(6), value(7), value(8))
    })

    val schema = StructType(
      //      StructField("省份", StringType) ::
      //        StructField("市", StringType) ::
      StructField("AppName", StringType) ::
        StructField("原始请求", DoubleType) ::
        StructField("有效请求", DoubleType) ::
        StructField("广告请求数", DoubleType) ::
        StructField("参与竞价数", DoubleType) ::
        StructField("竞价成功数", DoubleType) ::
        StructField("广告消费", DoubleType) ::
        StructField("广告成本", DoubleType) ::
        StructField("展示数", DoubleType) ::
        StructField("点击数", DoubleType) :: Nil)
    // val RptDF: DataFrame = sqlContext.createDataFrame(RptRow, schema)

    val AppDF: DataFrame = sqlContext.createDataFrame(AppRow, schema)

    AppDF.show()


    sc.stop()


  }
}
