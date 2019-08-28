package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.Tags
  * @date:2019/8/23
  */
object TagZCP extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    //地域标签
    //省标签
    val rtbprovince = row.getAs[String]("provincename")
    if (StringUtils.isNotBlank(rtbprovince)){
      list:+=("ZP"+rtbprovince,1)
    }
    //市标签
    val rtbcity = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(rtbcity)){
      list:+=("ZC"+rtbcity,1)
    }
    list
  }
}
