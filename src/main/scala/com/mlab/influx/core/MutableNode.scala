package com.mlab.influx.core

import org.apache.spark.util.AccumulatorV2
/**
  * Created by ravi on 4/4/17.
  */
abstract class MutableNode[A, B, C] extends Node[A, B] {
  val state: AccumulatorV2[C, C]
  def update(in: C) : Unit
}

object MutableNode {
}
