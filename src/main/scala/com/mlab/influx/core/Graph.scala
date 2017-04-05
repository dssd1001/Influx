package com.mlab.influx.core


/**
  * Represents an edge from one node to another, internally to a Graph
  */
private[core] case class Edge(from: Node[Any, Any], to: Node[Any, Any])

/**
  * Graph structure ot hold all processing nodes and connections in a graph.
  */
private[core] class Graph(nodes: Seq[Node[Any, Any]], edges: Seq[Edge],
            defaultInput: Option[Node[Any, Any]], defaultOutput: Option[Node[Any, Any]] = None) {

  /**
    * Connect a node to this graph. If there is a default output, this node is the new output.
    * @param node Node to add to end of graph.
    * @return Graph with node connected to the end.
    */
  def connect(node: Node[Any, Any]): Graph = {
    defaultOutput match {
      case Some(output) => new Graph(nodes :+ node, edges :+ new Edge(output, node), defaultInput, Some(node))
      case None => new Graph(nodes :+ node, edges, defaultInput, defaultOutput)
    }
  }

  /**
    * Set the default output node of the graph.
    * If node does not exist in the graph, connect it and make this the output.
    */
  def withDefaultOutput(node: Node[Any, Any]) : Graph = {
    if (nodes.contains(node)) {
      new Graph(nodes, edges, defaultInput, Some(node))
    } else {
      connect(node).withDefaultOutput(node)
    }
  }

  /**
    * Set the default input of the graph.
    * If node does not exist in the graph, connect it and make it the default input.
    */
  def withDefaultInput(node: Node[Any, Any]): Graph = {
    if (nodes.contains(node)) {
      new Graph(nodes, edges, Some(node), defaultOutput)
    } else {
      defaultOutput match {
        case Some(output) => connect(node).removeEdge(output, node).withDefaultInput(node)
        case None => connect(node).withDefaultInput(node)
      }
    }
  }

  /**
    * Disconnect two nodes that are connected in the graph.
    */
  private def removeEdge(from: Node[Any, Any], to: Node[Any, Any]): Graph = {
    new Graph(nodes, edges.filterNot(edge => edge.from == from && edge.to == to), defaultInput, defaultOutput)
  }


}
