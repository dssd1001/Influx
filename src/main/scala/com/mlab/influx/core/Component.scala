package com.mlab.influx.core

/**
  * Created by noahg on 4/4/2017.
  */
class Component extends Graph {
  private var nodes = Seq[Node[Any, Any]]()
  private var edges = Seq[Edge]()

  val input: Node[Any, Any]
  val output: Node[Any, Any]

  private val defaultInput = Some(input)
  private val defaultOutput = Some(output)

  def hook(from: Node[Any, Any], to: Node[Any, Any]): Unit = {
    if (!nodes.contains(from)) nodes = nodes :+ from
    if (!nodes.contains(to)) nodes = nodes :+ to
    edges = edges :+ Edge(from, to)
  }

  def addNode(node: Node[Any, Any]): Unit = {
    if (!nodes.contains(node))
      nodes = nodes :+ node
  }

}
