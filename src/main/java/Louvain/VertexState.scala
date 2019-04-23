package Louvain

/**
  * Created with IntelliJ IDEA.
  * Created by @author ChangMin Zhao 
  * Description: 
  * User: 59771
  * Date: 2018-08-09
  * Time: 17:28
  * Usage: 节点基础类
  */
class VertexState {
  var community:Long = -1L
  var communitySigmaTot = 0L
  var internalWeight = 0L
  var nodeWeight = 0L
  var changed=false

  override def toString: String = {
    "{community:"+community+", communitySigmaTot:"+communitySigmaTot+
    "， internalWeight："+internalWeight+", nodeWeight:"+nodeWeight+"}"
  }

}
