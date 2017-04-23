package com.mlab.influx.core

import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

sealed trait Operator {
  type IN
  type OUT
}

/**
  * A single processing node.
  * Processes an element of a stream using some function.
  */
abstract class Node[A, B: ClassTag] extends Operator with java.io.Serializable {
  override type IN = A
  override type OUT = B

  def apply(in: A) : B
  def apply(inStream: DStream[A]) : DStream[B] = inStream.map(apply)
}

object Node {
  /**
    * This constructor takes a function and returns a Node that maps it over the input stream
    *
    * @param f The function to apply to every item in the RDD being transformed
    * @tparam A input type of the node
    * @tparam B output type of the node
    * @return Node that applies the given function to all items in the RDD
    */
  def apply[A, B: ClassTag](f: A => B): Node[A, B] = new Node[A, B] {
    override def apply(in: DStream[A]): DStream[B] = in.map(f)
    override def apply(in: A): B = f(in)
  }
}

class StreamNode[A: ClassTag](stream: DStream[A]) extends Node[A, A] {
  def apply(in: A): A = in
  override def apply(inStream: DStream[A]) : DStream[A] = stream
}