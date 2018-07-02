package com

import com.CLI._

object Main {

  def main(args: Array[String]): Unit = {

    val result = command(args)

    result.fold(nel => {
      println(nel.list.toList.mkString("\n"))
    }, _ => {
      println("Success")
    })

  }

}
