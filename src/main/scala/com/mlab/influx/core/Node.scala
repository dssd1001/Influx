package com.mlab.influx.core

import org.apache.spark.streaming.dstream.DStream

/**
  * Created by suneelbelkhale1 on 3/14/17.
  */
abstract class Node[A, B] {
  protected var out: Option[DStream[B]] = None
  protected var prevNode: Option[Node[Any, A]] = None

  def apply(in: A) : B
  def apply(inStream: DStream[A]) : DStream[B] = inStream.map(apply)

  private[core] def connect(in: Node[Any, A]): Node[A,B] = {
      this.out = in.out match {
        case Some(stream) => Some(apply(stream))
        case None => throw new UnsupportedOperationException("Must connect to connected node")
      }
      this.prevNode = Some(in)
      this
  }
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
  def apply[A, B](f: A => B): Node[A, B] = new Node[A, B] {
    override def apply(in: DStream[A]): DStream[B] = in.map(f)
    override def apply(in: A): B = f(in)
  }
}