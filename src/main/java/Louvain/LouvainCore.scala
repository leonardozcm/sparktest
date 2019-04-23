package Louvain

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import spire.ClassTag

/**
  * Created with IntelliJ IDEA.
  * Created by @author ChangMin Zhao 
  * Description: 
  * User: 59771
  * Date: 2018-08-09
  * Time: 17:41
  * Usage: sparktest
  */
object LouvainCore {
  /**
    * 用来计算使用Graph结构
    * mapreducetriplets是对aggregatemessage的又一层封装
    */
  def creatLouvainGraph[VD: ClassTag](graph: Graph[VD, Long]):
  Graph[VertexState, Long] = {
    val nodeWeightMapFuncEX = (ctx: EdgeContext[VD, Long, (Long, Long)]) => {
      ctx.sendToDst((ctx.attr, 0L)) //msg:(A,即此处的（long 入度，long 出度)键值对
      ctx.sendToSrc((0L, ctx.attr))
    }
    val nodeWeightReduceFunc = (e1: (Long, Long), e2: (Long, Long)) => (e1._1 + e2._1, e1._2 + e2._2)
    val nodeWeights = graph.aggregateMessages(nodeWeightMapFuncEX, nodeWeightReduceFunc)

    /**
      * weightOption（Long，Long）
      * vid顶点的编号
      * data边的权值
      */
    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse((0L, 0L))
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = weight._1
      state.internalWeight = weight._1
      state.nodeWeight = weight._2
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    louvainGraph
  }

  def louvain(sc: SparkContext, graph: Graph[VertexState, Long], minProgress: Int = 1,
              progressCounter: Int = 1): (Double, Graph[VertexState, Long], Int) = {
    var louvainGraph = graph.cache()
    val graphWeight = louvainGraph.vertices.values.map(vdata =>
      vdata.internalWeight + vdata.nodeWeight).reduce(_ + _)
    var totalGraphWeight = sc.broadcast(graphWeight)
    println("totalEdgeWeight:" + totalGraphWeight.value)

    //收集每个节点的邻居节点社区ID、社区权重并累加，为后面判断所属社区准备
    var msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg)
    var activeMessages = msgRDD.count()

    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIter = 100000
    var stop = 0
    var updateLastPhase = 0L
    do {
      count += 1
      even = !even

      //计算当前节点所属最佳社区
      val labelVert = louvainVerJoin(louvainGraph, msgRDD, totalGraphWeight, even).cache()

      //统计所有社区的权重
      val communityUpdate = labelVert.map({ case (vid, vdata) =>
        (vdata.community, vdata.nodeWeight + vdata.internalWeight)
      }).reduceByKey(_ + _).cache()

      //将节点id与新社区id、权重关联
      val communityMapping = labelVert
        .map({ case (vid, vdata) => (vdata.community, vid) })
        .join(communityUpdate)
        .map({ case (community, (vid, sigmaTot)) =>
          (vid, (community, sigmaTot))
        }).cache()

      //更新VertexState值，建立节点id=》新VertexState映射
      val updatedVerts = labelVert.join(communityMapping).map({ case (vid, (vdata, communityTuple)) =>
        vdata.community = communityTuple._1
        vdata.communitySigmaTot = communityTuple._2
        (vid, vdata)
      }).cache()
      updatedVerts.count()
      labelVert.unpersist(false)
      communityUpdate.unpersist(false)
      communityMapping.unpersist(false)

      //得到更新后的Graph
      val preG = louvainGraph
      louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) =>
        newOpt.getOrElse(old))
      louvainGraph.cache()

      //更新msgRDD用于下次迭代，并将不用的临时表释放
      val oldMsgs = msgRDD
      msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg).cache()
      activeMessages = msgRDD.count()
      oldMsgs.unpersist(false)
      updatedVerts.unpersist(false)
      preG.unpersistVertices(false)

      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count()
      if (!even) {
        println("  #vertices moved: " +
          java.text.NumberFormat.getInstance().format(updated))
        if (updated >= updateLastPhase - minProgress) stop += 1
        updateLastPhase = updated
      }

    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))

    println("\nCompleted in " + count + " cycles")

    //重新计算整体图的模块度
    val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      val community = vdata.community
      var sigmaTot = vdata.communitySigmaTot.toDouble
      var k_i_in = vdata.internalWeight
      msgs.foreach({ case ((communityId, sigmaTotal), communityEdgeWeight) =>
        if (vdata.community == communityId) k_i_in += communityEdgeWeight
      })

      val M = totalGraphWeight.value
      val k_i = vdata.nodeWeight + vdata.internalWeight
      //计算新模块度
      var q = (k_i_in.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
      if (q < 0) 0 else q

    })
    val actualQ = newVerts.values.reduce(_ + _)
    (actualQ, louvainGraph, count / 2)
  }

  private def sendMsg(et: EdgeContext[VertexState, Long, Map[(Long, Long), Long]]) = {
    et.sendToDst(msg = Map((et.dstAttr.community, et.dstAttr.communitySigmaTot) -> et.attr))
    et.sendToSrc(msg = Map((et.srcAttr.community, et.srcAttr.communitySigmaTot) -> et.attr))
  }

  private def mergeMsg(m1: Map[(Long, Long), Long], m2: Map[(Long, Long), Long]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, Long), Long]()
    m1.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }

  private def louvainVerJoin(louvainGraph: Graph[VertexState, Long]
                             , msgRDD: VertexRDD[Map[(Long, Long), Long]]
                             , totalEdgeWeight: Broadcast[Long]
                             , even: Boolean) = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      var bestCommunity = vdata.community
      var startingCommunityId = bestCommunity
      //BigDevimal是精确计算类
      var maxDeltaQ = BigDecimal(0.0)

      var bestSigmaTot = 0L
      msgs.foreach({
        case ((communityId, signTotal), communityEdgeWeught) =>
          val deltaq = deltaQ(startingCommunityId, communityId, signTotal, communityEdgeWeught, vdata.nodeWeight, vdata.internalWeight, totalEdgeWeight.value)
          if (deltaq > maxDeltaQ || (deltaq > 0 && (deltaq == maxDeltaQ && communityId > bestCommunity))) {
            maxDeltaQ = deltaq
            bestCommunity = communityId
            bestSigmaTot = signTotal
          }

      })

      if (vdata.community != bestCommunity && ((even && vdata.community > bestCommunity) || (!even && vdata.community < bestCommunity))) {
        vdata.community = bestCommunity
        vdata.communitySigmaTot = bestSigmaTot
        vdata.changed = true
      }
      else {
        vdata.changed = false
      }
      vdata
    })

  }

  private def deltaQ(currCommunityId: Long
                     , testCommunity: Long
                     , testSigmaTot: Long
                     , edgeWeightInCommunity: Long
                     , nodeWeight: Long
                     , internalWeight: Long
                     , totalEdgeWeight: Long): BigDecimal = {
    val isCurrentCommunity = currCommunityId.equals(testCommunity)
    val M = BigDecimal(totalEdgeWeight)
    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity
    val k_i_in = BigDecimal(k_i_in_L)
    val k_i = BigDecimal(nodeWeight + internalWeight)
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot)

    var deltaQ = BigDecimal(0.0)
    if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
      deltaQ = k_i_in - (k_i * sigma_tot / M)
    }
    deltaQ

  }

  def compressGraph(graph: Graph[VertexState, Long], debug: Boolean = true): Graph[VertexState, Long] = {
    //边缘的新增加的社区权重
    val internalEdgeWeights = graph.triplets.flatMap(et => {
      if (et.srcAttr.community == et.dstAttr.community) {
        Iterator((et.srcAttr.community, 2 * et.attr))
      }
      else Iterator.empty
    }).reduceByKey(_ + _)
    //社区原本的权重
    var internalWeights = graph.vertices.values.map(vdata => (vdata.community, vdata.internalWeight)).reduceByKey(_ + _)

    //创建新的顶点RDD
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({ case (vid, (weight1, weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1 + weight2
      state.nodeWeight = 0L
      (vid, state)
    }).cache()

    //创建新的边RDD
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()

    //创建压缩后的图
    val compressedGraph = Graph(newVerts, edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    //重新统计点的权重
    val nodeWeightMapFuncEX = (ctx: EdgeContext[VertexState, Long, Long]) => {
      ctx.sendToDst(ctx.attr) //msg:(A,即此处的（long 入度，long 出度)键值对
      ctx.sendToSrc(ctx.attr)
    }
    val nodeWeightReduceFunc = {
      (e1: Long, e2: Long) => e1 + e2
    }
    val nodeWeights = graph.aggregateMessages(nodeWeightMapFuncEX, nodeWeightReduceFunc)

    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight + data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph

    newVerts.unpersist(blocking = false)
    edges.unpersist(blocking = false)
    louvainGraph
  }
}
