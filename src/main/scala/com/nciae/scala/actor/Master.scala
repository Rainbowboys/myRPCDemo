package com.nciae.scala.actor

/**
 * Created by Rainbow on 2016/11/11.
 */

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import akka.actor.Actor
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.concurrent.duration._

class Master(val host: String, val port: Int) extends Actor {
  println("cus Invoke")

  /**
   * worker id 和WorkerInfo之间的映射
   */
  val idToWorker = new HashMap[String, WorkerInfo]()
  /**
   * 将workerInfo 保存在set集合中
   */

  val workers = new HashSet[WorkerInfo]()

  val CHECK_OUT = 10 * 1000
  val CHECK_INTERVAL = 10 * 1000

  override def preStart(): Unit = {
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, MasterCheckOutTimeWorker)
  }

  override def receive: Receive = {
    case RegisterWorker(id, memery, cores) => {
      if (!idToWorker.contains(id)) {
        val workerInfo = new WorkerInfo(id, memery, cores)
        idToWorker(id) = workerInfo
        workers += workerInfo
      }

      /**
       * 恢复注册结果消息
       */
      sender ! RegisteredWorker(s"akka.tcp://MasterSystem@$host:$port/user/Master")
    }
    case HearBeat(workerId) => {
      if (idToWorker.contains(workerId)) {
        //更新最后一次心跳时间
        idToWorker(workerId).lastHeartBeat = System.currentTimeMillis()
        //        val updateworker = workers.filter(w => w.id == workerId).toArray
        //        println(updateworker(0).lastHeartBeat)
      }
    }
    case MasterCheckOutTimeWorker => {

      val currentTime = System.currentTimeMillis()
      val toRemove = workers.filter(w => currentTime - w.lastHeartBeat > CHECK_OUT).toArray
      for (t <- toRemove) {
        workers -= t
        idToWorker.remove(t.id)
      }
      println("workerSize:" + workers.size)
    }
//    case "check" => {
//      import context.dispatcher
//      context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, MasterCheckOutTimeWorker)
//    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
                                                   |akka.remote.netty.tcp.port = "$port"
       """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    /**
     * 创建老大 Actor
     */
    val actorSystem = ActorSystem("MasterSystem", config)

    /**
     * Create new actor as child of this context with the given name,
     */
    val master = actorSystem.actorOf(Props(new Master(host, port)), "Master")
    /**
     * 优雅的退出
     */
    actorSystem.awaitTermination()
  }
}