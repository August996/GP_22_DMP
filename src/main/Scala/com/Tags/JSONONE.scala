package com.Tags
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ListBuffer

object ExamTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val jsonRDD = sc.textFile("src/data/json.txt")
    val jsonStr: Array[String] = jsonRDD.collect()
    val jsonArr: Array[JSONObject] = jsonStr.map(json => {
      JSON.parseObject(json)
    })
    val array: Array[ListBuffer[(String, Int)]] = jsonArr.map(jsonparse => {
      val buffer = collection.mutable.ListBuffer[String]()
      val status = jsonparse.getIntValue("status")
      if (status == 0) return "1"
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return "2"
      val poiArray = regeocodeJson.getJSONArray("pois")
      if (poiArray == null || poiArray.isEmpty) return "3"
      for (item <- poiArray.toArray()) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
            buffer.append(json.getString("businessarea"))
        }
      }
      val tuples = buffer.map(x => (x, 1))
      tuples
    })
    val BUSSArr: Array[(String, Int)] = array.map(x => {
      val array: Array[(String, Int)] = x.toArray
      (array(0)._1, array.size)
    }).filter(x => !x._1.contains("[]"))
    println(BUSSArr.toBuffer)
  }

}