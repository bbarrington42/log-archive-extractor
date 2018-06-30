package com

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.model.{ListObjectsRequest, ListObjectsV2Request}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.util.Filtering.{isConsumerLog, isDataMessage}
import com.util.S3ObjectIterator.{asJsObjects, getContentAsString}
import org.joda.time.DateTime

import scala.collection.JavaConverters._

object Main2 {

  import com.util.PrefixUtil._

  def printlogs(s3: AmazonS3, env: String, start: DateTime, end: DateTime): Unit = {
    try {
      val ps = prefixes(start, end)
      ps.foreach(p => {
        // Get all the keys at the specified prefix
        val request = new ListObjectsRequest().
          withBucketName("cda_logs").withDelimiter("/").withPrefix(env + p)
        val response = s3.listObjects(request)
        println(response.getObjectSummaries.asScala)

//        val s3Object = s3.getObject("cda_logs", env + p)
//        val s = getContentAsString(s3Object)
//        val jsObjects = asJsObjects(s).filter(jsObject => {
//          val p = for {
//            b1 <- isDataMessage(jsObject)
//            b2 <- isConsumerLog(jsObject)
//          } yield b1 && b2
//          p.getOrElse(false)
//        })
//
//        println(jsObjects)
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

    val earlier = now.minusDays(2)

    printlogs(s3, "prod-green", earlier, now)
  }

}
