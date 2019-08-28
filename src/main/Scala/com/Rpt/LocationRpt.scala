package com.Rpt

import com.util.RptUtils
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
/**
  * 地域分布指标
  */
object LocationRpt {
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

    val df: DataFrame = sqlContext.read.parquet(inputPath)
//    df.registerTempTable("t_log")
//    val t_count: DataFrame = sqlContext.sql("select " +
//      "case when requestmode=1 and processnode>=1 then 1 else 0 end ysrq, " +
//      "case when requestmode=1 and processnode>=2 then 1 else 0 end sxrq, " +
//      "case when requestmode=1 and processnode=3 then 1 else 0 end mzadrq, " +
//      "case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end cypt, " +
//      "case when iseffective=1 and isbilling=1 and iswin=1 and adorderid != 0 then 1 else 0 end successpt, " +
//      "case when requestmode=2 and iseffective=1 then 1 else 0 end clickrealshow, " +
//      "case when requestmode=3 and iseffective=1 then 1 else 0 end clickreallook, " +
//      "case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end winPrice, " +
//      "case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end adpayment " +
//      "from t_log")
//    t_count.registerTempTable("t_count")
//    sqlContext.sql("select " +
//      "sum(ysrq) ysrq, " +
//      "sum(sxrq) sxrq, " +
//      "sum(mzadrq) mzadrq, " +
//      "sum(cypt) cypt, " +
//      "sum(successpt) successpt, " +
//      "sum(clickrealshow) clickrealshow, " +
//      "sum(clickreallook) clickreallook, " +
//      "sum(winPrice) winPrice, " +
//      "sum(adpayment) adpayment " +
//      "from t_count").show()
    //统计各个指标
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
      //key 值 地域省市
//      val pro = row.getAs[String]("provincename")
//      val city = row.getAs[String]("cityname")
//      val channelid = row.getAs[String]("channelid")
  //val ispname = row.getAs[String]("ispname")
  var networkmannername = row.getAs[String]("networkmannername")
      //创建三个对应方法处理指标
      val list1: List[Double] = RptUtils.request(requestmode, processnode)
      val list2: List[Double] = RptUtils.click(requestmode, iseffective)
      val list3: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      //  ((pro, city), list1 ::: list2 ::: list3)
      if (networkmannername.equals("未知")){networkmannername = "其他"}
      ((networkmannername), list1 ::: list2 ::: list3)

    })
    //val req1: RDD[((String, String), List[Double])] = req.reduceByKey((x, y)=>(x.zip(y)).map(x=>x._1+x._2))
    val req1 = req.reduceByKey((x,y)=>(x.zip(y)).map(x=>x._1+x._2))
    val RptRow: RDD[Row] = req1.map(row => {
      val value = row._2
      //Row(row._1._1, row._1._2, value(0), value(1), value(2), value(3), value(4), value(5), value(6), value(7), value(8))

      Row(row._1, value(0), value(1), value(2), value(3), value(4), value(5), value(6), value(7), value(8))


    })

    val schema = StructType(
//      StructField("省份", StringType) ::
//        StructField("市", StringType) ::
       // StructField("媒体ID", StringType) ::
      //  StructField("运营", StringType) ::
        StructField("联网方式", StringType) ::
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

    //val ChaDF: DataFrame = sqlContext.createDataFrame(RptRow, schema)
   // val CPADF: DataFrame = sqlContext.createDataFrame(RptRow, schema)
    val NetNameDF: DataFrame = sqlContext.createDataFrame(RptRow, schema)
    NetNameDF.show()
   // val RptDF: DataFrame = sqlContext.createDataFrame(RptRow, schema)


    sc.stop()




  }
}
