package com.mlab.influx.core

import javassist.bytecode.stackmap.TypeTag

import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe.ClassSymbol

/**
  * Represents an edge from one node to another, internally to a Graph
  */
private[core] case class Edge(from: Operator, to: Operator)


/**
  * Graph structure ot hold all processing nodes and connections in a graph.
  */
class Graph(nodesSeq: Seq[Operator], edgesSeq: Seq[Edge],
            defaultIn: Option[Operator] = None, defaultOut: Option[Operator] = None) extends Structure {

  nodes = nodesSeq
  edges = edgesSeq
  defaultInput = defaultIn
  defaultOutput = defaultOut

  private def nodeTypes[A, B](node: Node[A, B]) = (ru.weakTypeOf[A].typeSymbol.asClass, ru.weakTypeOf[B].typeSymbol.asClass)

  /**
    * Connect a node to this graph. If there is a default output, this node is the new output.
    * @param node Node to add to end of graph.
    * @return Graph with node connected to the end.
    */
  def connect[A, B](node: Node[A, B]): Graph = {
    //val (a, b) = nodeTypes(node)
    defaultOutput match {
      case Some(output) => new Graph(nodes :+ node, edges :+ new Edge(output, node),
        defaultInput, Some(node))
      case None => new Graph(nodes :+ node, edges, defaultInput, defaultOutput)
    }
  }

  def connect[A, B](existingNode: Operator, newNode: Node[A, B]): Graph = {
    new Graph(nodes :+ newNode, edges :+ Edge(existingNode, newNode), defaultInput, defaultOutput)
  }

  /**
    * Set the default output node of the graph.
    * If node does not exist in the graph, connect it and make this the output.
    */
  def withDefaultOutput[A, B](node: Node[A, B]) : Graph = {
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
  def withDefaultInput[A, B](node: Node[A, B]): Graph = {
    if (nodes.contains(node)) {
      new Graph(nodes, edges, Some(node), defaultOutput)
    } else {
      defaultOutput match {
        case Some(output) => connect(node).removeEdge(output, node).withDefaultInput(node)
        case None => connect(node).withDefaultInput(node)
      }
    }
  }

  def numberOfNodes:Int = {
    nodes.size
  }

  def numberOfEdges:Int = {
    edges.size
  }

  /**
    * Disconnect two nodes that are connected in the graph.
    */
  private def removeEdge(from: Operator, to: Operator): Graph = {
    new Graph(nodes, edges.filterNot(edge => edge.from == from && edge.to == to), defaultInput, defaultOutput)
  }

}

object Graph {
  def empty(): Graph = new Graph(Seq(), Seq())
  def connect[A, B](node: Node[A,B]): Graph = empty().connect(node)
}