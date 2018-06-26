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

  def isDataMessage(jsObject: JsObject): Boolean = {
    val p = for {
      jsv <- jsObject.value.get("messageType")
      msgType <- jsv.asOpt[String]
    } yield (msgType == "DATA_MESSAGE")
    p.getOrElse(false)
  }

  def isLogType(jsObject: JsObject, logType: LogType): Boolean = {
    val p = for {
      jsv <- jsObject.value.get("logGroup")
      lt <- jsv.asOpt[String]
    } yield lt.startsWith(logType.prefix)
    p.getOrElse(false)
  }

}
