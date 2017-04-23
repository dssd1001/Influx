package com.mlab.influx.core

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable

/**
  * Created by noahg on 4/20/2017.
  */
class ComponentTests extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = _
  var math : MathComponent = _

  before {
    val conf = new SparkConf().setAppName("Node tests").setMaster("local[1]")
    sc = new SparkContext(conf)
    math = new MathComponent()
  }

  test("Component does math") {
    val result: Int = math.extractFunction().apply(1)
    assert(result == 3)
  }

  test("Component does streaming math") {
    val rdd1 = sc.parallelize(List(1, 2, 3))
    val rdd2 = sc.parallelize(List(4, 5, 6))
    val ssc = new StreamingContext(sc, Seconds(1))
    val stream = ssc.queueStream[Int](mutable.Queue(rdd1, rdd2))
    val func: Node[Int, Int] = math.extractFunction()

    val resultStream = func.apply(stream).foreachRDD(rdd => {
      val vals = rdd.collect().toSeq
      assert(vals == List(3,5,7) || vals == List(9,11,13))
    })
  }

  after {
    sc.stop()
  }
}

class MathComponent extends Component {
  val multNode : Node[Int, Int] = Node(x => x * 2)
  val addNode : Node[Int, Int] = Node(x => x + 1)

  this.hook(multNode, addNode)
  this.setDefaultInput(multNode)
  this.setDefaultOutput(addNode)
}