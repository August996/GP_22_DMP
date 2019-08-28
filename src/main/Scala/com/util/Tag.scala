package com.util

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.util
  * @date:2019/8/23
  */
trait Tag {
  /**
    * 打标签的统一接口
    */
  def makeTags(args:Any*):List[(String,Int)]

}
