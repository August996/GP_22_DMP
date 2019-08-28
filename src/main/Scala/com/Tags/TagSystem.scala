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
object TagSystem extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //解析参数
    val row = args(0).asInstanceOf[Row]
    //设备标签   操作系统  联网方式   运营商
    //操作系统
    val client = row.getAs[Int]("client")
    client match {
      case v if v == 1 => list:+=("D00010001",1)
      case v if v == 2 => list:+=("D00010002",1)
      case v if v == 3 => list:+=("D00010003",1)
      case _ => list:+=("D00010004",1)
    }
    //联网方式
    val networkmannername = row.getAs[String]("networkmannername")
    if (StringUtils.isNotBlank(networkmannername)){
      networkmannername match {
        case v if v.equalsIgnoreCase("WIFI") => list:+=("D00020001",1)
        case v if v.equalsIgnoreCase("4G") => list:+=("D00020002",1)
        case v if v.equalsIgnoreCase("3G") => list:+=("D00020003",1)
        case v if v.equalsIgnoreCase("2G") => list:+=("D00020004",1)
        case _ => list:+=("D00020005",1)
      }
    }
    //运营商
    val ispname = row.getAs[String]("ispname")
    if (StringUtils.isNotBlank(ispname)){
      ispname match {
        case v if v.equals("移动") => list :+= ("D00030001", 1)
        case v if v.equals("联通") => list :+= ("D00030002", 1)
        case v if v.equals("电信") => list :+= ("D00030003", 1)
        case _ => list :+= ("D00030004", 1)
      }
    }
    list
  }
}
