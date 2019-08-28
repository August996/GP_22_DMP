package com.Tags

import com.typesafe.config.ConfigFactory
import com.util.{AppDictUtil, HbaseUtil, SeqUtil, TagUtils}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Tags
  * @date:2019/8/23
  */

/**
  * 标签上下文
  */
object TagsContext2 {
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

    val baseRDD = df.filter(TagUtils.OneuserId)
      //所有标签都在内部实现
      .map(row => {
  //取出用户id
   val userId: List[String] = TagUtils.getAllUserId(row)
      (userId,row)
    })
    //构建点集合
    val vertexRDD: RDD[(VertexId, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
      //  接下来通过row数据 打上所有标签（按照要求）
      val AD: List[(String, Int)] = TagsAd.makeTags(row)
      val App: List[(String, Int)] = TagApp.makeTags(row)
      val Adp: List[(String, Int)] = TagAdp.makeTags(row)
      val System: List[(String, Int)] = TagSystem.makeTags(row)
      val KeyWords: List[(String, Int)] = TagKeyWords.makeTags(row, stop_dict)
      val BusinessT: List[(String, Int)] = BusinessTag.makeTags(row)
      val ZCP: List[(String, Int)] = TagZCP.makeTags(row)
      val AllTag = AD ++ App ++ System ++ KeyWords ++ ZCP ++BusinessT
      //list((Sting,Int))
      //保证其中一个点携带所有标签，同时也保密userId
      val VD: List[(String, Int)] = tp._1.map((_, 0)) ++ AllTag
     // println(VD.toBuffer)
      //处理所有点的集合
      tp._1.map(uid => {
        if (tp._1.head.equals(uid)) {
          (uid.hashCode.toLong, VD)
        } else {
          (uid.hashCode.toLong, List.empty)
        }
      })
    })
    //构建边的集合
    val edgs: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uid => {
        Edge(tp._1.head.hashCode, uid.hashCode, 0)
      })
    })

    //构键图
    val graph: Graph[List[(String, Int)], Int] = Graph(vertexRDD,edgs)

    //取出顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    vertices.join(vertexRDD).map{
      case(userId,(conId,vd))=>{
        (conId,vd)
      }
    }.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
//数据存入 hbase
        .map{
      case (userid,userTag)=>{
        val put = new Put(Bytes.toBytes(userid))
        //处理标签
        val tags = userTag.map(x => x._1+":"+x._2).mkString(",")
        put.addImmutable(Bytes.toBytes("Tags"),Bytes.toBytes(days),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobconf)

sc.stop()


  }
}
