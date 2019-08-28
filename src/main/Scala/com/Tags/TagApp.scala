package com.Tags

import com.util.{AppDictUtil, Tag}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Tags
  * @date:2019/8/23
  */
object TagApp extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //val App_dict = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    //App标签

    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")
    //获取App_dict 根据App_dict判断AppName
    if (appname.equals("其他")){
     // list:+=("APP"+App_dict.value.getOrElse(appid,"其他"), 1)
     list:+=("APP"+AppDictUtil.getDict(appid), 1)

    }else{
      list:+=("APP"+appname, 1)
    }

    list
  }
}
