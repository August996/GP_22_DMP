package com.Tags

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer
/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Tags
  * @date:2019/8/24
  */
object JSONTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val jsonRDD = sc.textFile("src/data/json.txt")
    val jsonArr: Array[JSONObject] = jsonRDD.collect().map(json => {
      JSON.parseObject(json)
    })

    //创建LIST数据
    var list = List[String]()


    //JOSN解析
    jsonArr.foreach(jsonparse => {
      val status = jsonparse.getIntValue("status")
      if (status == 0) return "1"
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return "2"
      val poiArray = regeocodeJson.getJSONArray("pois")
      if (poiArray == null || poiArray.isEmpty) return "3"
      val buffer = collection.mutable.ListBuffer[String]()
      for (item <- poiArray.toArray()) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          //找到
          buffer.append(json.getString("type"))
        }
      }
      list:+=buffer.mkString(";")
    })


    //标签
    makeTags(list)


  }

  def makeTags(list:List[String]): Unit ={
    val BUSS: Map[String, Int] = list.flatMap(x => x.split(";")).map(x => ("BUSS:" + x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.length)
    BUSS.foreach(println(_))
  }

}
