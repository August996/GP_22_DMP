package com.util

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.util
  * @date:2019/8/21
  */
object AppDictUtil {
  //val jedis: Jedis = new Jedis("hadoop01",6379)
  def setDict(sc:SparkContext,dirPath:String) ={
    //获取字典，广播
    //    val arr: RDD[Array[String]] = sc.textFile("src/data/app_dict.txt").map(line => {
    //      line.split("\t")
    //    }).filter(line => line.length != 0)
    //    val app_dict: RDD[(String, String)] = arr.map(x => {
    //      val appName = x(1)
    //      val appIp = x(4)
    //      (appIp -> appName)
    //    })
    //    val appMap_dict = app_dict.collect().toMap
    //    val App_dict: Broadcast[Map[String, String]] = sc.broadcast(appMap_dict)
    val arr: RDD[Array[String]] = sc.textFile(dirPath).map(line => {
      line.split("\t")
    }).filter(line => line.length != 0)
    val app_dict: RDD[(String, String)] = arr.map(x => {
      val appName = x(1)
      val appIp = x(4)
      (appIp -> appName)
    })
    import scala.collection.JavaConverters._
    //val app_dictMap: util.Map[String, String] = app_dict.collect.toMap.asJava
    val app_dictMap: Map[String, String] = app_dict.collect().toMap
    //jedis.hmset("app",app_dictMap)
    JedisConnectionPool.hset("app",app_dictMap)
  }

def getDict(appid:String)={
      //jedis.hget("app",appid)
      //jedis.hgetAll("app")
  JedisConnectionPool.hget("app",appid)
}

}
