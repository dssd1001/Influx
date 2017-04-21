package com.mlab.influx.core

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

/**
  * Created by noahg on 4/20/2017.
  */
class Graph extends FunSuite with BeforeAndAfter {

  var g:Graph = _
  vasr sc: SparkContext = _


  before {
    val conf = new SparkConf().setAppName("Node tests").setMaster("localhost")
    sc = new SparkContext(conf)
    var n : Node = Node((x:Int)=>x+1)
    g = new Graph(new List(n), new List(), new Map(n, (Int, Int)))
  }

  test("extract function works properly") {
    assert(g.extractFunction(n,n).apply(1)==2)
  }

  test("adding node to graph") {
    var n2  = Node(x=>x+2)
    g.connect(n2)
    assert(g.nodes.size==2)
    assert(g.edges.size==1)
    assert(g.isConnected(n,n2))
  }

  test("composition of operators with multiple nodes") {
    assert(g.extractFunction(n,n2).apply(1)==4)
  }

  after {
    sc.stop()
  }

}
