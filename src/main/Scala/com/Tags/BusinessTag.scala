package com.Tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConnectionPool, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Tags
  * @date:2019/8/26
  */
object BusinessTag extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //获取经纬度 过滤经纬度
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    if (Utils2Type.toDouble(long)>= 73.0 &&
      Utils2Type.toDouble(long)<= 135.0 &&
      Utils2Type.toDouble(lat)>=3.0 &&
      Utils2Type.toDouble(lat)<= 54.0){
      //通过经纬度获取商圈
      //获取商圈
      val business: String = getBusiness(long.toDouble,lat.toDouble)
      //判断缓存中是否有此商圈
      if(StringUtils.isNotBlank(business)){
        //通过经纬度获取商圈
        val lines = business.split(",")
        lines.foreach(x => {

          list:+=(x,1)
        })
      }
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long:Double,lat:Double):String={
    //转换GeoHash换算
    val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    //println("geo:"+geohash)
    //数据库查询
    var business: String = redis_queryBusiness(geohash)
    //println("redis查询business:"+business)
    //判断商圈是否为空
    if(business == null || business.length == 0){
      //通过经纬度获取商圈
      business = AmapUtil.getBusinessFromAmap(long.toDouble,lat.toDouble)
      //
      //println("工具类:"+business)

        save2Redis(geohash,business)
    }
    business
  }
  def redis_queryBusiness(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business: String = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存储商圈到Redis
    */
  def save2Redis(geohash:String,business:String) ={
    val jedis: Jedis = JedisConnectionPool.getConnection()
      jedis.set(geohash,business)
    jedis.close()
  }
}
