package com.mlab.influx.core

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by noahg on 4/20/2017.
  */
class ComponentTests extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = _
  var math : MathComponent = _

  before {
    val conf = new SparkConf().setAppName("Node tests").setMaster("localhost")
    sc = new SparkContext(conf)
  }

  test("Component does math") {
    assert(math.extractFunction().apply(1) == 3)
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