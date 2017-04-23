package com.mlab.influx.core

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.mutable

/**
  * Created by noahg on 4/20/2017.
  */
class GraphTests extends FunSuite with BeforeAndAfter {

  var g:Graph = _
  var sc: SparkContext = _
  var n:Node[Int,Int] = _
  var n2:Node[Int,Int] = _
  var stream:DStream[Int] = _



  before {
    val conf = new SparkConf().setAppName("Graph tests").setMaster("local[1]")
    sc = new SparkContext(conf)
    n = Node((x:Int)=>x+1)
    g = Graph.connect(n)
    val rdd1 = sc.parallelize(List(1,2,3))
    val rdd2 = sc.parallelize(List(4,5,6))
    val ssc = new StreamingContext(sc,Seconds(1))
    stream = ssc.queueStream[Int](mutable.Queue(rdd1,rdd2))

  }

  test("extract function works properly for single element") {
    g = g.withDefaultInput(n)
    g = g.withDefaultOutput(n)
    val i:Int = g.extractFunction().apply(1)
    assert(i==2)
  }

  test("extract function works for streaming") {
    val resultNode:Node[Int,Int] = g.extractFunction(Some(n), Some(n))
    val resultStream = resultNode.apply(stream).foreachRDD(rdd=> {
      val vals = rdd.collect().toSeq
      assert(vals==List(2,3,4) || vals==List(5,6,7))
    })
  }

  test("adding node to graph") {
    n2  = Node((x:Int)=>x+2)
    g = g.withDefaultOutput(n)
    g = g.connect(n2)
    val i:Int = g.numberOfNodes
    val j:Int = g.numberOfEdges
    assert(i==2)
    assert(j==1)
    assert(g.isConnected(n,n2))
  }

  test("composition of operators with multiple nodes") {
    n2  = Node((x:Int)=>x+2)
    g = g.withDefaultOutput(n)
    g = g.connect(n2)
    val i:Int = g.extractFunction(Some(n),Some(n2)).apply(1)
    assert(i==4)
  }

  test("composition of operators with multiple nodes in streaming setting") {
    n2  = Node((x:Int)=>x+2)
    g = g.withDefaultOutput(n)
    g = g.connect(n2)
    val resultNode:Node[Int,Int] = g.extractFunction(Some(n), Some(n2))
    val resultStream = resultNode.apply(stream).foreachRDD(rdd=> {
      val vals = rdd.collect().toSeq
      assert(vals==List(4,5,6) || vals==List(7,8,9))
    })
  }

  after {
    sc.stop()
  }

}
