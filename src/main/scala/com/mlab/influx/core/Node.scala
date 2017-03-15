package com.mlab.influx.core

import org.apache.spark.streaming.dstream.DStream

/**
  * Created by suneelbelkhale1 on 3/14/17.
  */
class Node[A,B](applyFunc: Function[A,B]) extends Graph[A,B]{

  var out: DStream[B] = null
  var prevNode: Node[Any,A] = null
  override var outNode: ConnectedNode[A, B] = new ConnectedNode[A,B](this)

  override private[core] def connect(in: Graph[Any, A]): ConnectedNode[A,B] = {
      this.out = apply(in.outNode.out.asInstanceOf[DStream[A]])
      prevNode = in.outNode.asInstanceOf[Node[Any,A]]
      return new ConnectedNode[A,B](this)
  }

  def apply(in: A) : B = applyFunc(in)

  def apply(inStream: DStream[A]) : DStream[B] = inStream.map(apply)

}
