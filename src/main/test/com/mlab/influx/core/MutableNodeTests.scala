package com.mlab.influx.core

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable
/**
  * Created by noahg on 4/20/2017.
  */
class MutableNodeTests extends FunSuite with BeforeAndAfter {
  var sumAddNode: MutableNode[Int, Int, Int] = _
  var sc: SparkContext = _

  before {
    val conf = new SparkConf().setAppName("Mutable node tests").setMaster("local[1]")
    sc = new SparkContext(conf)

    sumAddNode = new MutableNode[Int, Int, Int] {
      override var initialState: Int = 0

      override def update(in: Int): Unit = state.add(state.value + in)

      override def apply(in: Int): Int = state.value + in
    }
  }

  test("Mutable Node adds a number") {
    assert(sumAddNode.apply(2) == 2)
  }

  test("Mutable Node update-add works") {
    sumAddNode.update(2)
    assert(sumAddNode.apply(3) == 5)
    assert(sumAddNode.state.value == 2)
  }

  test("Mutable node apply-update-apply-update") {
    sumAddNode.update(2)
    assert(sumAddNode.state.value == 2)
    assert(sumAddNode.apply(3) == 5)
    sumAddNode.update(3)
    assert(sumAddNode.state.value == 5)
    assert(sumAddNode.apply(2) == 7) // using previous test
  }

  test("Mutable node update on streams") {
    val rdd1 = sc.parallelize(List(1, 2, 3))
    val rdd2 = sc.parallelize(List(4, 5, 6))
    val rdd3 = sc.parallelize(List(0, 0))
    val ssc = new StreamingContext(sc, Seconds(1))
    sc.register(sumAddNode.state)
    val updateStream = ssc.queueStream[Int](mutable.Queue(rdd1, rdd3))
    sumAddNode.update(updateStream)

    ssc.start()
    Thread.sleep(2000)
    assert(sumAddNode.state.value == 6)
  }

  after {
    sc.stop()
  }
}
