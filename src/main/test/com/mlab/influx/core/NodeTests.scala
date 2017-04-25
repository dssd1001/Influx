package com.mlab.influx.core

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable

/**
  * Created by noahg on 4/20/2017.
  */
class NodeTests extends FunSuite with BeforeAndAfter {
  var singleElemAddNode: Node[Int, Int] = _
  var sc: SparkContext = _

  before {
    val conf = new SparkConf().setAppName("Node tests").setMaster("local[1]")
    sc = new SparkContext(conf)

    singleElemAddNode = Node(x => x + 1)
  }

  test("Node adds a number") {
    assert(singleElemAddNode.apply(2) == 3)
  }

  test("Node adds many numbers in two RDDs") {
    val rdd1 = sc.parallelize(List(1, 2, 3))
    val rdd2 = sc.parallelize(List(4, 5, 6))
    val ssc = new StreamingContext(sc, Seconds(1))
    val stream = ssc.queueStream[Int](mutable.Queue(rdd1, rdd2))
    val resultStream = singleElemAddNode.apply(stream).foreachRDD(rdd => {
      val vals = rdd.collect().toSeq
      assert(vals == List(2,3,4).toSeq || vals == List(5,6,7).toSeq)
    })

    ssc.start()
    Thread.sleep(2000)

  }
  after {
    sc.stop()
  }
}
