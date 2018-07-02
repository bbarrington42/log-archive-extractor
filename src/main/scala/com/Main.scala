package com

import com.CLI._

object Main {

  def main(args: Array[String]): Unit = {

    val fileName = "data/log.txt"

    val result = invoke("prod-green", "consumer", "2018/06/30/19", "2018/07/01/02", fileName)

    result.fold(nel => {
      println(nel.list.toList.mkString("\n"))
    }, _ => {
      println(s"Log contents written to $fileName")
    })

  }

}
