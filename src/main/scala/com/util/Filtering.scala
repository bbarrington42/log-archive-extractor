package com.util

import play.api.libs.json.JsObject

object Filtering {

  sealed trait LogType {
    val prefix: String
  }

  case object ConsumerLog extends LogType {
    override val prefix: String = "consumer"
  }

  case object AccessLog extends LogType {
    override val prefix: String = "access"
  }

  case object DispenserLog extends LogType {
    override val prefix: String = "dispenser"
  }

  sealed trait MessageType {
    val text: String
  }

  case object DataMessage extends MessageType {
    override val text: String = "DATA_MESSAGE"
  }

  case object ControlMessage extends MessageType {
    override val text: String = "CONTROL_MESSAGE"
  }

  val environments = Set("dev", "test", "prod-blue", "prod-green")
  val logTypes = Set(ConsumerLog.prefix, AccessLog.prefix, DispenserLog.prefix)

  def isDataMessage(jsObject: JsObject): Option[Boolean] = matchMessageType(jsObject, DataMessage)

  def isControlMessage(jsObject: JsObject): Option[Boolean] = matchMessageType(jsObject, ControlMessage)

  def matchMessageType(jsObject: JsObject, messageType: MessageType): Option[Boolean] = for {
    jsv <- jsObject.value.get("messageType")
    msgType <- jsv.asOpt[String]
  } yield msgType == messageType.text

//  def isAccessLog(jsObject: JsObject): Option[Boolean] = matchLogType(jsObject, AccessLog)
//
//  def isConsumerLog(jsObject: JsObject): Option[Boolean] = matchLogType(jsObject, ConsumerLog)
//
//  def isDispenserLog(jsObject: JsObject): Option[Boolean] = matchLogType(jsObject, DispenserLog)

  def matchLogType(jsObject: JsObject, logType: LogType): Option[Boolean] = for {
    jsv <- jsObject.value.get("logGroup")
    lt <- jsv.asOpt[String]
  } yield lt.startsWith(logType.prefix)

}
