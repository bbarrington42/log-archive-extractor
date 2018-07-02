package com

import java.io.{BufferedWriter, File, FileWriter, Writer}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.util.Filtering.{ConsumerLog, LogType, isDataMessage, matchLogType}
import com.util.S3ObjectIterator
import com.util.S3ObjectIterator.{asJsObjects, getContentAsString}
import org.joda.time.DateTime

object Main {

  import com.util.PrefixUtil._

  def fileWriter(file: File)(f: Writer => Unit) = {
    val writer = new BufferedWriter(new FileWriter(file))
    try {
      f(writer)
    } catch {
      case thr: Throwable => println(thr)
    } finally {
      writer.close()
    }
  }

  def extractLogs(s3: AmazonS3, env: String, logType: LogType, start: DateTime, end: DateTime, destination: File): Unit = {

    fileWriter(destination)(
      writer => {
        val ps = prefixes(env, start, end)
        ps.foreach(p => {
          val s3Iter = S3ObjectIterator(s3, "cda_logs", p)
          while (s3Iter.hasNext) {
            val s = getContentAsString(s3Iter.next())
            val jsObjects = asJsObjects(s).filter(jsObject => {
              val p = for {
                b1 <- isDataMessage(jsObject)
                b2 <- matchLogType(jsObject, logType)
              } yield b1 && b2
              p.getOrElse(false)
            })

            writer.write(jsObjects.mkString("\n", "\n", "\n"))
          }
        })
      }
    )

  }


  def main(args: Array[String]): Unit = {

    val credentialsProvider = new ProfileCredentialsProvider("cda")

    val s3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider)

    val s3 = s3ClientBuilder.build()

    val now = DateTime.now()

    val earlier = now.minusHours(0)

    val fileName = "data/log.txt"

    extractLogs(s3, "prod-green", ConsumerLog, earlier,
      now, new File(fileName))

    println(s"Log contents written to $fileName")
  }

}
