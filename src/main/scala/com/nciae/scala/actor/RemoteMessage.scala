package com.nciae.scala.actor

/**
 * Created by Rainbow on 2016/11/11.
 */
trait RemoteMessage extends Serializable

case class RegisterWorker(id: String, memery: Int, cores: Int)

case class RegisteredWorker(masterUrl: String) extends Serializable
case object SendHeartMessage

case class HearBeat(id: String) extends Serializable


case object MasterCheckOutTimeWorker