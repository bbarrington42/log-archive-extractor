package com

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.util.Filtering.{isConsumerLog, isDataMessage}
import com.util.S3ObjectIterator
import com.util.S3ObjectIterator.{asJsObjects, getContentAsString}
import org.joda.time.DateTime

object Main {

  import com.util.PrefixUtil._

  def printlogs(s3: AmazonS3, env: String, start: DateTime, end: DateTime): Unit = {
    try {
      val ps = prefixes(env, start, end)
      ps.foreach(p => {
        val s3Iter = S3ObjectIterator(s3, "cda_logs", p)
        while (s3Iter.hasNext) {
          val s = getContentAsString(s3Iter.next())
          val jsObjects = asJsObjects(s).filter(jsObject => {
            val p = for {
              b1 <- isDataMessage(jsObject)
              b2 <- isConsumerLog(jsObject)
            } yield b1 && b2
            p.getOrElse(false)
          })

          println(jsObjects.mkString("\n", "\n", "\n"))
        }
      })
    } catch {
      case ex: Exception => println(ex)
    }

  }


  def main(args: Array[String]): Unit = {

    val credentialsProvider = new ProfileCredentialsProvider("cda")

    val s3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider)

    val s3 = s3ClientBuilder.build()

    val now = DateTime.now()

    val earlier = now.minusHours(0)

    printlogs(s3, "prod-green", earlier, now)
  }

}
