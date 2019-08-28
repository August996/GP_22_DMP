package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Tags
  * @date:2019/8/23
  */
object TagKeyWords extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    //停用词库
    val stopKeyWords: Broadcast[Map[String, Int]] = args(1).asInstanceOf[Broadcast[Map[String,Int]]]

    //关键字标签
    val keywords = row.getAs[String]("keywords")
    if (StringUtils.isNotBlank(keywords)){
      if (keywords.contains("|")){
        val keywordArr: Array[String] = keywords.split("\\|")
        for ( key <- keywordArr ){
          if (key.length>=3 && key.length <=8 && !stopKeyWords.value.contains(key)){
            list:+=("K"+key,1)
          }
        }

      }else{
        if (keywords.length>=3 && keywords.length <=8 && !stopKeyWords.value.contains(keywords)){
          list:+=("K"+keywords,1)
        }
      }
    }
    list
  }
}
