package com.mlab.influx.core

import scala.collection.mutable.ArrayBuffer

/**
  * Created by suneelbelkhale1 on 4/16/17.
  */
abstract class Structure {
  protected var nodes: Seq[Operator] = ArrayBuffer.empty[Operator]
  protected var edges: Seq[Edge] = ArrayBuffer.empty[Edge]
  protected var defaultInput: Option[Operator] = None
  protected var defaultOutput: Option[Operator] = None

  /**
    *
    * @param op1
    * @param op2
    * @return
    */
  private def isConnected(op1: Operator, op2: Operator): Boolean = {
    if (!nodes.contains(op1) || !nodes.contains(op2)) return false

    // TODO: TEST THIS -> connectedness tests
    // similar code as in extractFunction, but not the same due to return statements
    var currOp = op2

    while (!currOp.equals(op1)) {
//      val op = currOp
//      //add to list
//      var currNode = currOp.asInstanceOf[Node[op.IN,op.OUT]]
      //get an edge to, traversing backwards
      var next: Edge = edges.find((e: Edge) => e.to.equals(currOp)) match {
        case Some(edge) => edge
        case None => return false
      }
      //iterating through the edge list
      currOp = next.from
    }
    true
  }

  /**
    * constructs and returns a function that goes through the graph
    *   - Iterates backwards through edgeTo starting from end
    *   - when it reaches start (TODO: design for null or improper formed graphs)
    *     it then has finished creating a list of Operators / Nodes
    *   - passes this into curry, and then returns that in Node form
    *
    * @param start the beginning node
    * @param end the ending node
    * @tparam A type of input to start
    * @tparam B type of output of end
    * @return Node that is the composed function
    */
  def extractFunction[A, B](start: Option[Operator] = None, end: Option[Operator] = None): Node[A,B] = {
    assert {
      start.isDefined || defaultInput.isDefined
    }
    assert {
      end.isDefined || defaultOutput.isDefined
    }

    val startOp = start match {
      case Some(s) => s
      case None => defaultInput.get
    }

    val endOp = end match {
      case Some(e) => e
      case None => defaultOutput.get
    }

    assert {
      isConnected(startOp, endOp)
    }

    // TODO: TEST THIS, it is completely untested
    var currOp = endOp
    var list = List(endOp)

    while (!currOp.equals(startOp)) {
      val op = currOp
      //add to list
//      var currNode = currOp.asInstanceOf[Node[op.IN,op.OUT]]
      //get an edge to, traversing backwards
      var next: Edge = edges.find((e: Edge) => e.to.equals(currOp)) match {
        case Some(edge) => edge
        case None => throw Exception
      }
      //iterating through the edge list
      currOp = next.from
      //
      list = currOp :: list;
    }


    //construct the chained function
    def func = curry[A, A](list, 0, (x => x)).asInstanceOf[(A => B)];
    //construct the node
    return new Node[A,B] {
      override def apply(in: A) = func(in)
    }
  }

  /**
    * Curries a function and an element of a sequence of Operators together
    *
    * @param sequence full sequence of operators in order
    * @param index the current index operator to chain
    * @param previousFunction the function chain of all elements up to this point
    * @tparam A overall input param
    * @tparam IN input param to the sequence{index} node
    * @return the result of recursively currying
    */
  def curry[A, IN](sequence: Seq[Operator], index: Int, previousFunction: (A => IN)): (A => Any) = {
    if (index >= sequence.length) {
      return previousFunction;
    }
    val o = sequence{index}
    val node = o.asInstanceOf[Node[IN,o.OUT]]

    def nextFunc = (x: A) => node.apply(previousFunction(x))

    //recursive
    return curry[A, o.OUT](sequence, index + 1, nextFunc)
  }
}
