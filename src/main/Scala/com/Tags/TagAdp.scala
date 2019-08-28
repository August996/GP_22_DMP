package com.Tags

import com.util.Tag
import org.apache.spark.sql.Row

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Tags
  * @date:2019/8/23
  */
object TagAdp extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    //渠道标签
    val adp = row.getAs[Int]("adplatformproviderid")
    if (adp != 0){
      list:+=("CN"+adp,1)
    }
    list
  }
}
