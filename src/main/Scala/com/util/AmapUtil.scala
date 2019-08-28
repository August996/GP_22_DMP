package com.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.util
  * @date:2019/8/24
  */
/**
  * 商圈解析工具
  */
object AmapUtil {
    //获取高德地图商圈信息
  def getBusinessFromAmap(long:Double,lat:Double):String={

    val location = long + "," + lat
    println("location->"+location)
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=c0edb72a4601bf14a0fc75620f73bdeb&radius=1000&extensions=all"

    //调用请求
    val jsonStr = HttpUtil.get(urlStr)
    //解析json串
    val jsonparse: JSONObject = JSON.parseObject(jsonStr)
    //判断状态是否成功
    val status: Int = jsonparse.getIntValue("status")
    if (status == 0) return ""
    //解析内部json串,判断每个key的value都不能为空
    val regecodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
    if (regecodeJson == null || regecodeJson.keySet().isEmpty) return ""

    val addressComponentJson: JSONObject = regecodeJson.getJSONObject("addressComponent")
    if (addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray: JSONArray = addressComponentJson.getJSONArray("businessAreas")
    if (businessAreasArray == null || businessAreasArray.isEmpty) return ""
    //创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    //循环输出
    for (item <- businessAreasArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json: JSONObject = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))

      }
    }
    buffer.mkString(",")
  }
}
