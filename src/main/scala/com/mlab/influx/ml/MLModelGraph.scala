package com.mlab.influx.ml

import com.mlab.influx.core._
import org.apache.spark.streaming.dstream.DStream


abstract class MLModelGraph[A, B, M](trainStream: DStream[(A, B)], queryStream: DStream[A]) extends Component {

  val trainStreamNode = new StreamNode(trainStream)
  val queryStreamNode = new StreamNode(queryStream)

  val initialModel: M

  this.addNode(trainStreamNode)
  this.addNode(queryStreamNode)

  def learn(trainData: A, trainLabels: B, oldModel: M): M
  def predict(data: A, model: M): B

  val learner = new Node[(A, B, M), M] {
    override def apply(in: (A, B, M)): M = learn(in._1, in._2, in._3)
  }

  val server = new MutableNode[A, B, M] {
    override def apply(in: A): B = predict(in, state.value)
    override def update(in: M): Unit = state.add(in)
    override def initialState = initialModel
  }

  this.hook(trainStreamNode, learner)
  this.hook(queryStreamNode, server)

}
