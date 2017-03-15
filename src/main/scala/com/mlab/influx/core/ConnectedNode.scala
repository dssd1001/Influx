package com.mlab.influx.core
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by suneelbelkhale1 on 3/14/17.
  */
class ConnectedNode[A,B](node: Node[A,B]) extends Node[A,B]{
  override val out: DStream[B] = node.out
  override var prevNode: Node[Any, A] = node.prevNode

  override def apply(in: A): B = node.apply(in)
}
