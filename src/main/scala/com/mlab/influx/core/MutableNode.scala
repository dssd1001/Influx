package com.mlab.influx.core

import org.apache.spark.util.AccumulatorV2


class Accumulator[C](initialState: C) extends AccumulatorV2[C, C] {
  private var state: C = initialState

  def reset(): Unit = { state = initialState}
  def add(in: C): Unit = {state = in }

  override def isZero: Boolean = false

  override def copy(): AccumulatorV2[C, C] = new Accumulator(state)

  override def merge(other: AccumulatorV2[C, C]): Unit = { state = other.value }

  override def value: C = state
}

/**
  * Created by ravi on 4/4/17.
  */
abstract class MutableNode[A, B, C] extends Node[A, B] {
  val initialState: C

  val state: Accumulator[C] = new Accumulator(initialState)
  def update(in: C) : Unit = { state.add(in) }
}

object MutableNode {
}
