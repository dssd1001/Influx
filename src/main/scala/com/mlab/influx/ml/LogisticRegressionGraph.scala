package com.mlab.influx.ml

import org.apache.spark.streaming.dstream.DStream
import breeze.linalg._
import breeze.math._
import breeze.numerics._
import operators._
import com.mlab.influx.core.{Node, StreamNode}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by ravi on 4/11/17.
  */
class LogisticRegressionGraph[A <: Vector[Double] : ClassTag](trainStream: DStream[(A, Int)], queryStream: DStream[A], size: Int, learningRate: Double)
  extends MLModelGraph[A, Int, A](trainStream, queryStream, Vector.zeros[Double](size).asInstanceOf[A]){

  val input = new StreamNode(queryStream)
  val output = new StreamNode(server.apply(queryStream))

  override def learn(trainData: A, trainLabel: Int, oldModel: A): A = {
    val z = trainLabel - sigmoid(oldModel dot trainData)
    val update = (learningRate * z) * trainData.toDenseVector;
    val newModel = (oldModel - update.asInstanceOf[A]).asInstanceOf[A];
    return newModel;
  }

  def learn(trainData: RDD[A], trainLabels: RDD[Int], oldModel: A): A = {
    val data = trainData.asInstanceOf[RDD[Vector[Double]]].collect()
    val realData : Array[Array[Double]] = new Array(data.length);

    for (i <- 0 until data.length) {
      realData(i) = data(i).toArray
    }
    val procData = DenseMatrix(realData.map(_.toArray):_*)
    val labels = DenseVector(trainLabels.collect())
    val z = labels.toDenseMatrix.asInstanceOf[DenseMatrix[Double]] - sigmoid(procData * oldModel.toDenseVector.toDenseMatrix)
    val newModel = oldModel.asInstanceOf[Vector[Double]].toDenseVector.toDenseMatrix - (procData * (learningRate * z))

    return newModel.toDenseVector.asInstanceOf[A];
  }

  override def predict(data: A, model: A): Int = {
    if(sigmoid(data dot model) < 0.5) -1 else 1
  }
  def predict(data: RDD[A], model: A): RDD[Int] = {
    data.map(
      x => if(sigmoid(x dot model) < 0.5) -1 else 1
    )
  }
}
