package Louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import spire.ClassTag

/**
  * Created with IntelliJ IDEA.
  * Created by @author ChangMin Zhao 
  * Description: 
  * User: 59771
  * Date: 2018-08-13
  * Time: 17:31
  * Usage: sparktest
  */
class HDFSLouvainRunner(minProgress: Int, progressCounter: Int, outputdir: String) {

  var qValues: Array[(Int, Double)] = Array[(Int, Double)]()

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]) = {
    var louvainGraph = LouvainCore.creatLouvainGraph(graph)

    var level = -1
    var q = -1.0
    var halt = false
    do {
      level += 1
      println(s"\nStartin Lounvain level $level")

      //调用Louvain算法计算每个节点所属的当前社区
      val (currentQ, currentGraph, passes) = LouvainCore.louvain(sc, louvainGraph, minProgress, progressCounter)
      louvainGraph.unpersistVertices(false)
      louvainGraph = currentGraph

      saveLevel(sc, level, currentQ, louvainGraph)

      //如果模块度增加量超过0.001，说明当前划分比前一次好，继续迭代
      //如果计算当前社区的迭代次数少于3次，就停止循环
      if (passes > 2 && currentQ > q + 0.001) {
        q = currentQ
        louvainGraph = LouvainCore.compressGraph(louvainGraph)
      }
      else {
        halt = true
      }
    } while (!halt)
    saveLevel(sc, level, q, louvainGraph)
  }

  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[VertexState, Long]) = {
    graph.vertices.saveAsTextFile(outputdir + "/level_" + level + "_vertices")
    graph.edges.saveAsTextFile(outputdir + "/level_" + level + "_edges")
    //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
    //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")
    qValues = qValues :+ ((level, q))
    println(s"qValue: $q")

    // overwrite the q values at each level
    sc.parallelize(qValues, 1).saveAsTextFile(outputdir + "/qvalues")
  }

}
