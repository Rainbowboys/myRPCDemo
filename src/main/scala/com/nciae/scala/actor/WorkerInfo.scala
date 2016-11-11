package com.nciae.scala.actor

/**
 * Created by Rainbow on 2016/11/11.
 */
class WorkerInfo(val id: String, val memery: Int, val cores: Int) {
  var lastHeartBeat: Long = System.currentTimeMillis()
}