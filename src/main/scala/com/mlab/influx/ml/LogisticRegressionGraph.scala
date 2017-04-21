package com.mlab.influx.ml

import org.apache.spark.streaming.dstream.DStream
import breeze.linalg._
import breeze.math._
import breeze.numerics._
import com.mlab.influx.core.{Node, StreamNode}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by ravi on 4/11/17.
  */
class LogisticRegressionGraph[A <: Vector[Double] : ClassTag](trainStream: DStream[(A, Int)], queryStream: DStream[A], size: Int, learningRate: Double)
  extends MLModelGraph[A, Int, A](trainStream, queryStream){

  val initialModel = DenseVector.zeros[Double](size)
  val input = new StreamNode(queryStream)
  val output = new StreamNode(server.apply(queryStream))

  override def learn(trainData: A, trainLabels: Int, oldModel: A): A = {
    val z = trainLabels - sigmoid(oldModel * trainData)
    oldModel - (learningRate * z) * trainData
  }

  def learn(trainData: RDD[A], trainLabels: RDD[Int], oldModel: A): A = {
    val data = DenseVector(trainData.collect())
    val labels = DenseVector(trainLabels.collect())
    val z = labels - sigmoid(data.t * oldModel)
    oldModel - (data.t * (learningRate * z))
  }

  override def predict(data: A, model: A): Int = {
    if(sigmoid(data * model).asInstanceOf[Double] < 0.5) -1 else 1
  }
  def predict(data: RDD[A], model: A): RDD[Int] = {
    data.map(
      x => if(sigmoid(x * model).asInstanceOf[Double] < 0.5) -1 else 1
    )
  }
}
