package com

import com.CLI._

object Main {

  def main(args: Array[String]): Unit = {

    val result = command(args)

    result.fold(nel => {
      println(nel.list.toList.mkString("\n"))
      println(CLI.usage)
    }, map => {
      if (map.contains("help")) println(CLI.usage) else
        println(s"Log entries written to ${map("to")}")
    })

  }

}
