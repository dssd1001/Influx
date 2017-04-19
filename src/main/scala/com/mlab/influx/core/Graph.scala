package com.mlab.influx.core

import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe.ClassSymbol

/**
  * Represents an edge from one node to another, internally to a Graph
  */
private[core] case class Edge(from: Operator, to: Operator)

//private[core] class GraphFunction[A,B](f: A=>B, parentGraph: Graph, dependencies: Seq[Operator]) extends Function[A,B] {
//  var currentF = f
//  var currentDependencies = dependencies
//
//  def updateFunc(): Unit = {
//
//  }
//
//  def dependenciesHaveChanged(): Boolean = false
//
//  override def apply(in: A): B = {
//    if (dependenciesHaveChanged()) updateFunc()
//    currentF(in)
//  }
//}


/**
  * Graph structure ot hold all processing nodes and connections in a graph.
  */
private[core] class Graph(nodesSeq: Seq[Operator], edgesSeq: Seq[Edge], types: Map[Operator, (ClassSymbol, ClassSymbol)],
            defaultIn: Option[Operator], defaultOut: Option[Operator] = None) extends Structure {

  nodes = nodesSeq
  edges = edgesSeq
  defaultInput = defaultIn
  defaultOutput = defaultOut

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

//  private def isConnected(op1: Operator, op2: Operator): Boolean = {
//    if (!nodes.contains(op1) || !nodes.contains(op2)) return false
//    // TODO: write connectedness tests
//    true
//  }

//  private def getComputationPath(fromOp: Operator, toOp: Operator): Seq[Operator] = {
//    getComputationPath(fromOp, toOp, Seq())
//  }

//  private def getComputationPath(fromOp: Operator, toOp: Operator, path: Seq[Operator]): Seq[Operator] = {
//    val fromFanoutNodes = edges.filter(_.from == fromOp).map(_.to)
//    val toFanoutNodes = edges.filter(_.from == toOp).map(_.to)
//
//    if (fromFanoutNodes.isEmpty) return path
//    if (toFanoutNodes.isEmpty) return path
//  }

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

//  def subgraph[A, B](start: Option[Operator] = None, end: Option[Operator] = None): GraphFunction[A,B] = {
//    assert {
//      start.isDefined || defaultInput.isDefined
//    }
//    assert {
//      end.isDefined || defaultOutput.isDefined
//    }
//
//    val startOp = start match {
//      case Some(s) => s
//      case None => defaultInput.get
//    }
//
//    val endOp = end match {
//      case Some(e) => e
//      case None => defaultOutput.get
//    }
//
//    assert {
//      isConnected(startOp, endOp)
//    }
//
//    val startNode = startOp.asInstanceOf[Node[startOp.IN, startOp.OUT]]
//    val endNode = startOp.asInstanceOf[Node[endOp.IN, endOp.OUT]]
//  }

}
