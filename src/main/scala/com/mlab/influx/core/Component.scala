package com.mlab.influx.core

import scala.collection.mutable.ArrayBuffer

/**
  * Created by noahg on 4/4/2017.
  */
class Component(input: Operator, output: Operator) {
  private val nodes: Seq[Operator] = ArrayBuffer.empty[Operator]
  private val edges: Seq[Edge] = ArrayBuffer.empty[Edge]

  private val defaultInput = Some(input)
  private val defaultOutput = Some(output)

  def hook(from: Operator, to: Operator): Unit = {
    /** CODE TO GET TYPES **/ 
    val fromNode = from.asInstanceOf[Node[from.IN,from.OUT]] 
    val toNode = to.asInstanceOf[Node[to.IN,to.OUT]]  

    if (!nodes.contains(from)) nodes :+ from
    if (!nodes.contains(to)) nodes :+ to
    edges :+ Edge(from, to)
  }

  def addNode(node: Operator): Unit = {
    if (!nodes.contains(node))
      nodes :+ node
  }

}
