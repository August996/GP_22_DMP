package com.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author: create by August
  * @version: v1.0
  * @description: com.graphx
  * @date:2019/8/26
  */
object Graph_test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //构造点的集合


    val vertexRDD: RDD[(VertexId, (String, Int))] = sc.makeRDD(
      List(
        (1L, ("詹姆斯", 22)),
        (2L, ("霍华德", 23)),
        (6L, ("杜兰特", 31)),
        (9L, ("库里", 30)),
        (133L, ("哈登", 30)),
        (138L, ("席尔瓦", 36)),
        (16L, ("法尔考", 35)),
        (44L, ("内马尔", 27)),
        (21L, ("J罗", 28)),
        (5L, ("高斯林", 55)),
        (7L, ("奥的司机", 55)),
        (158L, ("马云", 55))
      ))

//    val tuple: (VertexId, (String, Int)) = vertexRDD.min()
//    println(tuple)
    //构造边的集合
    val edge: RDD[Edge[Int]] = sc.makeRDD(List(
      Edge(1L, 133L, 1),
      Edge(2L, 133L, 1),
      Edge(6L, 133L, 1),
      Edge(9L, 133L, 1),
      Edge(6L, 138L, 1),
      Edge(16L, 138L,1),
      Edge(44L, 138L, 1),
      Edge(21L, 138L, 1),
      Edge(5L, 158L, 1),
      Edge(7L, 158L, 1)
    ))
    //构建图
      val graph: Graph[(String, Int), Int] = Graph(vertexRDD,edge)

    //取出每个边的最大顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
     vertices.join(vertexRDD)
//           .foreach(x => {
//       println(x)
//     })
       .map{
     case(userId,(conId,(name,age)))=>{
       (conId,List(name,age))
     }
   }.reduceByKey(_++_).foreach(println)
sc.stop()

  }
}
