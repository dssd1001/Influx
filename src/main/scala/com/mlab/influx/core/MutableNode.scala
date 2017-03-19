package com.mlab.influx.core

import org.apache.spark.util.AccumulatorV2

/**
  * Created by suneelbelkhale1 on 3/14/17.
  */
abstract class MutableNode[A, B, C] extends Node[A, B] {
  val state: AccumulatorV2[C, C]
  protected var rightNode: Option[Node[Any, C]]
  
  def update(in: C): Unit

  private[core] def connect(inLeft: Node[Any, A], inRight: Node[Any, C]): Node[A,B] = {
    connect(inLeft)
    this.rightNode = Some(inRight)
    inRight.out match {
      case Some(stream) => stream.foreachRDD(_.foreach(update))
      case None => throw new UnsupportedOperationException("Node must be connected")
    }
    this
  }
}
