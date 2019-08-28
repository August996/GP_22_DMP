package com.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.util
  * @date:2019/8/25
  */
object HbaseUtil {
  def getConnection() ={
    //1.读取配置文件
    val conf: Configuration = HBaseConfiguration.create()
    //建立zookepper连接
    conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //2。与HBase建立连接
     ConnectionFactory.createConnection(conf)
    //创建Admin
    //val admin = connection.getAdmin

    //定义表
    //val AdTags: TableName = TableName.valueOf("AdTags")
    //    //创建表的扫描器
    //    val AdTagsDesc = new HTableDescriptor(AdTags)
    //    //创建列族
    //    val TagsDesc: HColumnDescriptor = new HColumnDescriptor("Tags")
    //    //列族加入列表
    //    AdTagsDesc.addFamily(TagsDesc)
    //    //admin创建表
    //    admin.createTable(AdTagsDesc)
//    //定义表的类
//    val AdTagsTable: Table = connection.getTable(AdTags)
//    //rowkey
//    val put: Put = new Put(Bytes.toBytes("IM: 47544899705513"))
//    //获取列族信息
//    val familys: Array[HColumnDescriptor] = AdTagsTable.getTableDescriptor.getColumnFamilies
//    //遍历列族信息
//    for ( columnFamily <- familys){
//      //获取列族名
//      val cfName = columnFamily.getNameAsString
//      //family  qualifier value 列族 列 内容
//      put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes("LC01"),Bytes.toBytes(1))
//      AdTagsTable.put(put)



    }



  def save2Hbase(connection: Connection,tag:(String, List[(String, Int)]))={

      //定义表
      val AdTags: TableName = TableName.valueOf("AdTags")
      //定义表的类
      val AdTagsTable: Table = connection.getTable(AdTags)

      //rowkey
      val put: Put = new Put(Bytes.toBytes(tag._1.toString))
      //获取列族信息
      val familys: Array[HColumnDescriptor] = AdTagsTable.getTableDescriptor.getColumnFamilies
      //遍历列族信息
      for ( columnFamily <- familys){
        //获取列族名
        val cfName = columnFamily.getNameAsString
        //family  qualifier value 列族 列 内容
        tag._2.foreach(tag => {
          put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(tag._1.toString),Bytes.toBytes(tag._2.toInt))
          AdTagsTable.put(put)
        })
    }
  }
}

//(IM: 47544899705513,
// List((LC01,1),
// (LNbanner,1), (APP女性-and,1), (CN100016,1),
// 00010001,1), (D00020005,1), (D00030004,1), (ZP浙江省,1), (ZC绍兴市,1)))
//查询
//5 定义rowkey
//		Get rowkey=new Get(Bytes.toBytes("1001"));
//
//		//5 定义结果集
//		Result result=table.get(rowkey);
//		//6 定义单元格数组，并从result结果集中拿去数据转换单元格
//		Cell[] cells=result.rawCells();
//		//7 遍历单元格
//		for(Cell cell:cells) {
//			//取得内容 rowkey
//			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
//			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
//			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
//			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
//		}
//import org.apache.hadoop.hbase.util.Bytes
// 4 获得表的实例// 4 获得表的实例
//val table = connection.getTable(tableName)
//val delete = new Nothing(Bytes.toBytes("1001"))

//(IM: 47544899705513,
// List((LC01,1),
// (LNbanner,1), (APP女性-and,1), (CN100016,1),
// 00010001,1), (D00020005,1), (D00030004,1), (ZP浙江省,1), (ZC绍兴市,1)))


//查询
//5 定义rowkey

//		Get rowkey=new Get(Bytes.toBytes("1001"));
//
//		//5 定义结果集

//		Result result=table.get(rowkey);

//		//6 定义单元格数组，并从result结果集中拿去数据转换单元格
//		Cell[] cells=result.rawCells();

//		//7 遍历单元格

//		for(Cell cell:cells) {

//			//取得内容 rowkey

//			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));

//			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));

//			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));

//			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));


//		}

//import org.apache.hadoop.hbase.util.Bytes
// 4 获得表的实例// 4 获得表的实例


//val table = connection.getTable(tableName)

//val delete = new Nothing(Bytes.toBytes("1001"))
