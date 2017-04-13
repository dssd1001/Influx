package com.mlab.influx.ml

import org.apache.spark.streaming.dstream.DStream
import breeze.linalg._
import breeze.numerics._

/**
  * Created by ravi on 4/11/17.
  */
class LogisticRegressionGraph[A <: Vector, B <: Int, M <: Vector](trainStream: DStream[(A, B)], queryStream: DStream[A], size: Int)
  extends MLModelGraph[A, B, M](trainStream, queryStream){

  val initialModel = Vector.fill(size)(0)
  override def learn(trainData: A, trainLabels: B, oldModel: M)
}
