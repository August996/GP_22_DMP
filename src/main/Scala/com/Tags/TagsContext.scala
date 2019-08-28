package com.Tags

import java.util

import com.typesafe.config.ConfigFactory
import com.util.{AppDictUtil, HbaseUtil, TagUtils}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Tags
  * @date:2019/8/23
  */

/**
  * 标签上下文
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 5){
      println("目录不匹配，退出")
      sys.exit()
    }
    val Array(inputPath,outputPath,dirPath,stopPath,days)=args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //获取字典,将数据存入Redis
    AppDictUtil.setDict(sc,dirPath)

    //获取停用词库
    val stopMap = sc.textFile(stopPath).map((_,0)).collectAsMap()
    val stop_dict: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopMap)

    // todo 调用Hbase API
    // 加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.tableName")
    // 创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    // 判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("Tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    //读取数据
    val df: DataFrame = sqlContext.read.parquet(inputPath)
//    df.filter(TagUtils.OneuserId).registerTempTable("temp")
//    sqlContext.sql("select long,lat from temp where long > 0").show()
    //过滤符合Id的数据
    val tags = df.filter(TagUtils.OneuserId)
      //所有标签都在内部实现
      .map(row => {
      //去出用户id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row数据 打上所有标签（按照要求）
      val AD: List[(String, Int)] = TagsAd.makeTags(row)
      //val App: List[(String, Int)] = TagApp.makeTags(row, App_dict)
      val App: List[(String, Int)] = TagApp.makeTags(row)
      val Adp: List[(String, Int)] = TagAdp.makeTags(row)
      val System: List[(String, Int)] = TagSystem.makeTags(row)
      val KeyWords: List[(String, Int)] = TagKeyWords.makeTags(row, stop_dict)
      //val BusinessT: List[(String, Int)] = BusinessTag.makeTags(row)
      val ZCP: List[(String, Int)] = TagZCP.makeTags(row)
     // (userId, AD ::: App ::: Adp ::: System ::: KeyWords ::: BusinessT ::: ZCP ::: Nil)
      (userId, AD ::: App ::: Adp ::: System ::: KeyWords ::: ZCP ::: Nil)
    }).reduceByKey((list1, list2) => {
      (list1 ::: list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    })
    //方式2
//    .map{
//  case (userid,userTag)=>{
//    val put = new Put(Bytes.toBytes(userid))
//    //处理标签
//    val tags = userTag.map(x => x._1+":"+x._2).mkString(",")
//    put.addImmutable(Bytes.toBytes("Tags"),Bytes.toBytes(days),Bytes.toBytes(tags))
//    (new ImmutableBytesWritable(),put)
//  }
//}.saveAsHadoopDataset(jobconf)

    //方式1：数据存入Hbase
    tags.foreachPartition(tagsPartition =>{
     // println("**************************")
      val connection = HbaseUtil.getConnection()
      tagsPartition.foreach(x =>{
        if(!x._1.isEmpty){
          //HbaseUtil.save2Hbase(connection,x)
         // println(x.toString())
        }
      })
    })


tags.collect().foreach(x=>println(x))
sc.stop()


  }
}
