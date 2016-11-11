package com.nciae.scala.actor

/**
 * Created by Rainbow on 2016/11/11.
 */
import akka.actor.{ Props, ActorSystem, ActorSelection, Actor }
import akka.actor.Actor.Receive
import com.typesafe.config.ConfigFactory
import java.util.UUID
import scala.concurrent.duration._

/**
 * Created by Rainbow on 2016/11/10.
 */

class Worker(val masterHost: String, val masterPort: Int, val workerId: String, memery: Int, cores: Int) extends Actor {
  var master: ActorSelection = _
  val CHECK_INTERVAL = 5000

  override def preStart(): Unit = {
    /**
     * 向MasterActorSystem 请求 建立连接
     *
     */
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")

    /**
     * 发送注册信息
     */
    master ! RegisterWorker(workerId, memery, cores)

  }

  override def receive: Receive = {
    case RegisteredWorker(masterUrl) => {
      import context.dispatcher
      println(masterUrl)
      //发送心跳
      context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, SendHeartMessage)
    }
    case SendHeartMessage => {
      master ! HearBeat(workerId)
    }

  }
}

object Worker {

  def main(args: Array[String]) {
    val workerHost = args(0)
    val workerPort = args(1).toInt
    val masterHost = args(2)
    val masterPort = args(3).toInt
    val workerId = UUID.randomUUID().toString()
    val memery = args(4).toInt
    val cores = args(5).toInt

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$workerHost"
                                                         |akka.remote.netty.tcp.port = "$workerPort"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    /**
     * 创建老大 Actor
     */
    val actorSystem = ActorSystem("WorkerSystem", config)

    /**
     * Create new actor as child of this context with the given name,
     */
    val worker = actorSystem.actorOf(Props(new Worker(masterHost, masterPort, workerId, memery, cores)), "Worker")

    /**
     * 优雅的退出
     */
    actorSystem.awaitTermination()
  }

}
