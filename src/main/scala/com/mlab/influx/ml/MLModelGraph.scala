package com.mlab.influx.ml

import com.mlab.influx.core.{Component, Node, StreamNode}
import org.apache.spark.streaming.dstream.DStream


class MLModelGraph[A, B, M](trainStream: DStream[(A, B)], queryStream: DStream[A]) extends Component {

  val trainStreamNode = new StreamNode(trainStream)
  val queryStreamNode = new StreamNode(queryStream)

  this.addNode(trainStreamNode)
  this.addNode(queryStreamNode)

  def learn(trainData: A, trainLabels: B, oldModel: M): M
  def predict(data: A, model: M): B

  val learner = new Node[(A, B, M), M] {
    override def apply(in: (A, B, M)): M = learn(in._1, in._2, in._3)
  }

  val server = new MutableNode[A, B, M](())

  override val input: Node[Any, Any] = _
  override val output: Node[Any, Any] = _



  this.hook(trainStreamNode, learner)
  this.hook(queryStreamNode, server)


}
