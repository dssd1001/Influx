package com.mlab.influx.core

import scala.collection.mutable.ArrayBuffer

/**
  * Created by noahg on 4/4/2017.
  */
abstract class Component extends Structure{
//  private var nodes: Seq[Operator] = ArrayBuffer.empty[Operator]
//  private var edges: Seq[Edge] = ArrayBuffer.empty[Edge]
//
//  val input: Operator
//  val output: Operator

  def hook(from: Operator, to: Operator): Unit = {
    /** CODE TO GET TYPES **/ 
    val fromNode = from.asInstanceOf[Node[from.IN,from.OUT]] 
    val toNode = to.asInstanceOf[Node[to.IN,to.OUT]]  

    if (!nodes.contains(from)) nodes = nodes :+ from
    if (!nodes.contains(to)) nodes = nodes :+ to
    edges = edges :+ Edge(from, to)
  }

  def addNode(node: Operator): Unit = {
    if (!nodes.contains(node))
      nodes = nodes :+ node
  }
}
