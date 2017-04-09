package com.mlab.influx.core

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe.ClassSymbol

/**
  * Represents an edge from one node to another, internally to a Graph
  */
private[core] case class Edge(from: Operator, to: Operator)

/**
  * Graph structure ot hold all processing nodes and connections in a graph.
  */
private[core] class Graph(nodes: Seq[Operator], edges: Seq[Edge], types: Map[Operator, (ClassSymbol, ClassSymbol)],
            defaultInput: Option[Operator], defaultOutput: Option[Operator] = None) {

  private def nodeTypes[A, B](node: Node[A, B]) = (ru.typeOf[A].typeSymbol.asClass, ru.typeOf[B].typeSymbol.asClass)

  /**
    * Connect a node to this graph. If there is a default output, this node is the new output.
    * @param node Node to add to end of graph.
    * @return Graph with node connected to the end.
    */
  def connect[A, B](node: Node[A, B]): Graph = {
    val (a, b) = nodeTypes(node)
    defaultOutput match {
      case Some(output) => new Graph(nodes :+ node, edges :+ new Edge(output, node),
        types + (node -> (a, b)), defaultInput, Some(node))
      case None => new Graph(nodes :+ node, edges, types + (node -> (a,b)), defaultInput, defaultOutput)
    }
  }

  def connect[A, B](existingNode: Operator, newNode: Node[A, B]): Graph = {
    new Graph(nodes :+ newNode, edges :+ Edge(existingNode, newNode), types + (newNode -> nodeTypes(newNode)), defaultInput, defaultOutput)
  }

  /**
    * Set the default output node of the graph.
    * If node does not exist in the graph, connect it and make this the output.
    */
  def withDefaultOutput[A, B](node: Node[A, B]) : Graph = {
    if (nodes.contains(node)) {
      new Graph(nodes, edges, types, defaultInput, Some(node))
    } else {
      connect(node).withDefaultOutput(node)
    }
  }

  /**
    * Set the default input of the graph.
    * If node does not exist in the graph, connect it and make it the default input.
    */
  def withDefaultInput[A, B](node: Node[A, B]): Graph = {
    if (nodes.contains(node)) {
      new Graph(nodes, edges, types, Some(node), defaultOutput)
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
  private def removeEdge(from: Operator, to: Operator): Graph = {
    new Graph(nodes, edges.filterNot(edge => edge.from == from && edge.to == to), types, defaultInput, defaultOutput)
  }

  def compute(node: Node[Any, Any]): Any = {
    val incomingNodes = edges.filter(_.to == node).map(_.from)
    if (node.isInstanceOf[MutableNode]) {
      assert { incomingNodes.size == 2 }
      val leftNode = incomingNodes.head
      val rightNode = incomingNodes(1)

      val mutableNode = node.asInstanceOf[MutableNode]
      // TODO: get the return value here

    } else {
      assert { incomingNodes.size == 1}
      val prevNode = incomingNodes.head
      if (prevNode.isInstanceOf[StreamNode]) {
        val stream = prevNode.asInstanceOf[StreamNode].stream
        node.apply(stream)
      } else {
        node.apply(compute(prevNode))
      }
    }
  }


}
