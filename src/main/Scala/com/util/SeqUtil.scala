package com.util

import org.apache.spark.graphx.VertexId

/**
  * @author: create by August
  * @version: v1.0
  * @description: com.util
  * @date:2019/8/26
  */
object SeqUtil extends Serializable {
  def setSeq(seq:Seq[(VertexId, (String, List[(String, Int)]))]): Seq[(VertexId, (String, List[(String, Int)]))] ={
    val tuples: Seq[(VertexId, (String, List[(String, Int)]))] = seq.map(x => {
      (x._1, (x._2._1, x._2._2))
    })
    tuples
  }
}
