package com.mlab.influx.core

import scala.collection.mutable.ArrayBuffer

/**
  * Created by noahg on 4/4/2017.
  */
class Component extends Graph {
  private val nodes: Seq[Node[Any, Any]] = ArrayBuffer.empty[Node[Any, Any]]
  private val edges: Seq[Edge] = ArrayBuffer.empty[Edge]

  val input: Node[Any, Any]
  val output: Node[Any, Any]

  private val defaultInput = Some(input)
  private val defaultOutput = Some(output)

  def hook(from: Node[Any, Any], to: Node[Any, Any]): Unit = {
    if (!nodes.contains(from)) nodes += from
    if (!nodes.contains(to)) nodes += to
    edges += Edge(from, to)
  }

  def connect(leftNode: Node[Any, Any], rightNode: Node[Any, Any], mutableNode: MutableNode[A, B, C]): Unit = {

  }

  def addNode(node: Node[Any, Any]): Unit = {
    if (!nodes.contains(node))
      nodes = nodes :+ node
  }

}
