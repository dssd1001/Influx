package com.mlab.influx.core

/**
  * Created by suneelbelkhale1 on 3/14/17.
  */
trait Graph[TermIn, TermOut] {
  var outNode: ConnectedNode[TermIn, TermOut]
  private[core] def connect(in: Graph[Any, TermIn]): Graph[TermIn, TermOut] = outNode.connect(in)
}
